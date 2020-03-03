from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .data_sources import DataSourceInterface


class CountJob():

    def __init__(self, source: DataSourceInterface = None):
        self.source_data = source


    @staticmethod
    def count_type(event_type: str, with_consent=False):
        if with_consent:
            return F.count(F.when(
                (F.col('type') == event_type) & F.col('user.consent'), 1))
        return F.count(F.when(F.col('type') == event_type, 1))


    @classmethod
    def calcul_all_aggregates(cls, events: DataFrame) -> DataFrame:

        # Create a list of all aggregation functions
        exprs = [
            cls.count_type('pageview').alias('pageviews'),
            cls.count_type('pageview', with_consent=True)
                    .alias('pageviews_with_consent'),
            cls.count_type('consent.asked').alias('consents_asked'),
            cls.count_type('consent.asked', with_consent=True)
                    .alias('consents_asked_with_consent'),
            cls.count_type('consent.given').alias('consents_given'),
            cls.count_type('consent.given', with_consent=True)
                        .alias('consents_given_with_consent'),
            F.countDistinct('user.id').alias('users')
        ]

        groupby = ['datehour', 'domain', 'user.country']

        return events.groupby(groupby).agg(*exprs)


    def run(self):
        events = self.source_data.read_data()
        events = events.dropDuplicates(['id'])
        results = self.calcul_all_aggregates(events)

        return results
