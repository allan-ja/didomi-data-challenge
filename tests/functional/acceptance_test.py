from pyspark.sql import SparkSession

from challenge.data_sources import JSONHiveDataSource
from challenge.jobs import CountJob
from tests.helpers import areDataframesEquals


def test_givenJSONEventsData_should_return_aggregations():
    '''
    GIVEN events data as JSON, with duplicates
    WHEN CountJob is run
    THEN it should return required aggregates without the duplicates
    '''
    spark = SparkSession \
            .builder \
            .getOrCreate()

    expected_df = spark.createDataFrame(
        [
            ('2020-01-21-01', 'www.website.com', 'US', 2, 2, 0, 0, 0, 0, 2),
            ('2020-01-21-00', 'www.website.com', 'US', 2, 1, 1, 0, 1, 1, 1),
            ('2020-01-21-01', 'www.website.com', 'FR', 1, 1, 0, 0, 0, 0, 1),
            ('2020-01-21-01', 'www.other-website.com', 'FR', 1, 1, 0, 0, 0, 0, 1)
        ],
        [
            "pageviews_consent", 'pageviews_with_consent', 'consents_asked',
            'consents_asked_with_consent', 'consents_given',
            'consents_given_with_consent', 'users'
        ]
    )

    data_source = JSONHiveDataSource('tests/fixtures/input')
    sut = CountJob(data_source)
    result = sut.run()
    assert(areDataframesEquals(result, expected_df))
