from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    load_dimensions_sql = '''
        INSERT INTO {} 
        {};
        COMMIT;
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_stmt='',
                 append=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            self.log.info(f'Deleting {self.table} fact table')
            redshift.run(f'DELETE FROM {self.table}')
        self.log.info(f'Inserting data from fact tables in {self.table} fact table')
        formatted_sql = LoadDimensionOperator.load_dimensions_sql.format(
            self.table,
            self.sql_stmt
        )
        redshift.run(formatted_sql)