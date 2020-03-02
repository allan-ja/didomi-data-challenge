from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class DataSourceInterface(ABC):

    @abstractmethod
    def read_data(self):
        pass


class JSONHiveDataSource(DataSourceInterface):

    def __init__(self, data_path: str):
        self.data_path = data_path


    def read_data(self) -> DataFrame:
        spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()

        return spark.read.json(self.data_path)
