import pytest
from pyspark.sql import SparkSession, DataFrame

from challenge.data_sources import DataSourceInterface, JSONHiveDataSource
from challenge.jobs import CountJob
from tests.helpers import areDataframesEquals


@pytest.fixture()
def spark():
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .getOrCreate()
    return spark


@pytest.fixture()
def fake_events(spark):
    path = "tests/fixtures/input"
    return spark.read.json(path)


def test_should_count_type_pageviews_without_consent(spark, fake_events):

    expected_df = spark.createDataFrame([(7,)], ["pageviews"])
    result = fake_events.agg(CountJob.count_type('pageview').alias('pageviews'))

    assert(areDataframesEquals(result, expected_df))


def test_should_count_type_pageviews_with_consent(spark, fake_events):

    expected_df = spark.createDataFrame([(6,)], ["pageviews_consent"])
    result = fake_events.agg(
        CountJob.count_type('pageview', with_consent=True).alias('pageviews'))
    assert(areDataframesEquals(result, expected_df))


def test_should_CountJob_calcul_all_aggregates(spark, fake_events):
    expected_df = spark.createDataFrame(
        [
            ('2020-01-21-01', 'www.website.com', 'US', 2, 2, 0, 0, 0, 0, 2),
            ('2020-01-21-00', 'www.website.com', 'US', 3, 2, 1, 0, 1, 1, 1),
            ('2020-01-21-01', 'www.website.com', 'FR', 1, 1, 0, 0, 0, 0, 1),
            ('2020-01-21-01', 'www.other-website.com', 'FR', 1, 1, 0, 0, 0, 0, 1)
        ],
        [
            "pageviews_consent", 'pageviews_with_consent', 'consents_asked',
            'consents_asked_with_consent', 'consents_given',
            'consents_given_with_consent', 'users'
        ]
    )
    result = CountJob.calcul_all_aggregates(fake_events)
    assert(areDataframesEquals(result, expected_df))


def test_create_JSONHiveDataSource():
    sut = JSONHiveDataSource('dummy_path')
    assert(sut)
    assert(sut.data_path == 'dummy_path')


def test_JSONHiveDataSource_should_read_data(fake_events):
    data_folder = 'tests/fixtures/input'
    sut = JSONHiveDataSource(data_folder)
    data_read = sut.read_data()
    assert(type(data_read) == DataFrame)
    assert(data_read.subtract(fake_events).count() == 0)


def test_DataSourceInterface_should_raise_exception():
    with pytest.raises(TypeError):
        sut = DataSourceInterface()
