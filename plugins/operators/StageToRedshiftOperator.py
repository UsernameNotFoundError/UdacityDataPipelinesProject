from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    """Redshift operator 
    see here for more infos:
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_modules/airflow/providers/amazon/aws/transfers/s3_to_redshift.html
    """
    ui_color = '#78888'  # Color

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            aws_credentials_id="",
            sql_command="",
            s3_bucket_name = "",
            s3_key = "",
            *args, 
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        metastoreBackend = MetastoreBackend()
        self.aws_connection = metastoreBackend.get_connection(aws_credentials_id)
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.sql_command = sql_command if sql_command else   \
                f"""
                COPY staging_songs FROM 's3://{self.s3_bucket_name }/{self.s3_key}' 
                CREDENTIALS 'aws_access_key_id={self.aws_connection.login};aws_secret_access_key={self.aws_connection.password}'
                FORMAT AS json 'auto'
                TRUNCATECOLUMNS
                compupdate off region 'us-east-1';
                """
        

    def execute(self, context : dict) -> None:
        """Every operator must have an execute function
        _____
        arg: 
            - context : contains information abou the task (the execution time, configuration, ...)
        """
        try:
            self.log.info("Copying data from S3 to Redshift")
            self.log.info(vars(self.aws_connection))
            redshift_hook = PostgresHook("redshift")
            redshift_hook.run(self.sql_command)
            self.log.info("Finished Copying data from S3 to Redshift")
        except Exception as e:
            self.log.info(f"Error while Copying data from S3 to Redshift:\n{e}")
            assert()        





