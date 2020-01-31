import atexit
import sys
from typing import Dict

from pyspark import SparkConf
from pyspark.sql import SparkSession


class JobContext:
    def __init__(self, master: str, app_name: str = None, extra_args: Dict = None):
        """
        Initialize spark connection

        :param master: Spark master
        :param app_name: Set the app name. Only useful if running from shell
        :param extra_args: Extra arguments to be passed to the spark configuration
        """

        self.sc_conf = SparkConf()
        if master:
            self.sc_conf.setMaster(master)
        self.sc_conf.setAll([
                # ('spark.submit.pyFiles', 'jobs.zip'),
                ('spark.pyspark.driver.python', sys.executable)
            ])
        if app_name:
            self.sc_conf.setAppName(app_name)
        if extra_args:
            self.sc_conf.setAll([(key, value) for key, value in extra_args.items()])
        self.spark = SparkSession.builder.config(conf=self.sc_conf).getOrCreate()
        self.sc = self.spark.sparkContext

        # When the program exits, release the spark connection
        atexit.register(self.spark.stop)
