#!/usr/bin/env python3

import argparse
import importlib
import os
import zipfile

from driver_resources.job_context import JobContext


def main(main_args):
    """
    Run whatever job is specified by the first argument after main.py.
    """
    job_module = importlib.import_module(f'jobs.{main_args.job}')
    jc = JobContext(main_args.master, app_name=main_args.job)
    jc.spark.sparkContext.setLogLevel(main_args.log_level)
    job_module.analyze(jc, main_args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-level', nargs='?', type=str, default='ERROR',
                        help='Log level for this application, including Spark logs.')
    parser.add_argument('-m', '--master', nargs='?', type=str, default='local[*]',
                        help='Spark master, for local, set this to `local[n]`, where n is the number of cores to use.')

    # This will collect the first word after main.py, and store it in the parser's job field. It is important that
    # the subparser looks for the name of the module that the job is in, as importlib is used later to import the job
    # and will look for the job in the module specified by the job argument.
    subparsers = parser.add_subparsers(title='Application', description='Spark application to run.', dest='job')

    process_files_parser = subparsers.add_parser('guttenberg', help='Run Guttenberg project')

    process_files_parser.add_argument('-d', '--data', nargs='?', type=str, default='data/*.txt',
                                      help='Directory where data is stored.')

    args = parser.parse_args()

    main(args)
    exit(0)
