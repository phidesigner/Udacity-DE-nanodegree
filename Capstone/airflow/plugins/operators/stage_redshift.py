from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 aws_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 schema='',
                 options='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.options = options
        self.autocommit = True
        self.region = 'us-west-2'

    def execute(self, context):
        self.log.info('StageToRedshiftOperator in progress...')
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_options = '\n\t\t\t'.join(self.options)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE 'arn:aws:iam::739546605657:role/myRedshiftRole'
            {copy_options};
        """.format(schema=self.schema,
                   table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   copy_options=copy_options)

        self.log.info(f'Copying files from S3 bucket to Redshift')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("Transfer of data completed")
