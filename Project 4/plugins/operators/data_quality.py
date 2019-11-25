from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f'No records on table {table}')
                raise ValueError(f'Quality check failed. {table} returned no results')

            num_records = records[0][0]
            if num_records < 1:
                self.log.info(f'No rows on table {table}')
                raise ValueError(f'Quality check failed. {table} returned 0 rows')
            self.log.info(f'Data quality check on {table} passed with {num_records} records')
