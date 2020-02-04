from argparse import Namespace
from collections import Counter
from re import sub
import datetime as dt
from dateutil.parser import parse, ParserError
from pyspark.sql.functions import size, split
from pyspark.sql.types import StructType, StringType, StructField, TimestampType

from driver_resources.job_context import JobContext


def analyze(jc: JobContext, args: Namespace) -> None:
    """
    Starting point for Guttenberg project

    :param jc: JobContext containing all spark related objects for the driver
    :param args: argparse arguments passed into main function
    """

    data = jc.sc.textFile(args.data)
    # Put data in DataFrame. Local Checkpoint to keep it from being recreated on every access
    schema = StructType([
        StructField('title', StringType()),
        StructField('author', StringType()),
        StructField('release_date', TimestampType()),
        StructField('language', StringType()),
        StructField('text', StringType())
    ])
    data_df = jc.spark.createDataFrame(data.mapPartitions(parse_data), schema=schema).localCheckpoint()

    # The filter will keep texts that have the word truth separated by any number of characters at least three times
    multi_truth = data_df.filter(data_df.text.like('%truth%truth%truth%')).select('title').collect()
    print('In which books does the word “truth” appear more than twice?')
    print([title[0] for title in multi_truth])

    avg_release = data_df.agg({'release_date': 'avg'}).collect()[0][0]
    # avg will calculate the average unix timestamp, which might be a float, but needs to be int, so round
    converted_avg_release = dt.datetime.utcfromtimestamp(round(avg_release))
    print('What is the average release date? ')
    print(converted_avg_release)

    release_dates = data_df.select('release_date').collect()
    # The most common release date is None, so get the second most common
    # This is not the most scalable solution, as it has to pull all the release dates to the driver, but I couldn't
    # come up with anything else off the top of my head
    most_common_release_date = Counter(release_dates).most_common(2)[1][0][0]
    print('What is the most common release date?')
    print(most_common_release_date)

    english_books = data_df.filter(data_df.language == 'English').count()
    english_book_ratio = english_books/data_df.count()
    print('What is the ratio of English books to non English books?')
    print(english_book_ratio)

    # You could also do count by character, but the answer is the same for the given data set
    data_df = data_df.withColumn('word_count', size(split(data_df.text, '')))
    longest_book_by_word = data_df.sort(data_df.word_count).select('title', 'author').take(1)[0]
    print('Which book is the longest?')
    print(f'{longest_book_by_word[0]} by {longest_book_by_word[1]}')


def parse_data(data):
    """
    This function is meant to be used with mapPartitions. It will take a partition (single book's data), and parse
    out any fields in the format 'key: value'. It will also look for a line with '***' in it, which should signal the
    beginning of the text. It will take the book text, replace new lines with spaces, and strip extra whitespace.

    :param data: data from a single partition of the full data (information about a single book)
    :return: list containing fields that will be used to answer questions
    """
    if not data:
        return [[]]
    r = {}
    text = []
    for line in data:
        if ': ' in line:
            split_line = line.split(': ')
            r[split_line[0]] = split_line[1]
        if '***' in line:
            # The iterator has already gone through the lines up to the indicator of the start of the text, so extending
            # with it will only add the lines after the indicator.
            text.extend(data)
    release_date = None
    if r.get('Release Date'):
        # There a lot of release dates that have some extra data in brackets, remove that before trying to parse the
        # date. There are many more inconsistencies, but in the interest of time, just return None for those.
        try:
            release_date = parse(sub(r'\[.*\]', '', r.get('Release Date')))
        except ParserError:
            pass
    clean_text = sub(r' +', ' ', ' '.join(text).strip())
    return [[r.get('Title'), r.get('Author'), release_date, r.get('Language'), clean_text]]
