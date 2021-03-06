from pyspark.sql import SparkSession

from challenge.data_sources import JSONHiveDataSource
from challenge.jobs import CountJob


def main():
    spark = SparkSession \
        .builder \
        .appName("Didomi Data Challenge") \
        .getOrCreate()


    data_source = JSONHiveDataSource('tests/fixtures/input')
    job = CountJob(data_source)
    results = job.run()

    results.show()


if __name__ == "__main__":
    main()
