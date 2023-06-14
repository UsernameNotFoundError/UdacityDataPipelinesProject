from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


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
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.sql_command = \
                f"""
                COPY staging_songs FROM 's3://{self.s3_bucket_name }/{self.s3_key}' 
                CREDENTIALS 'aws_iam_role={self.aws_credentials_id}'
                FORMAT AS json 'auto'
                TRUNCATECOLUMNS
                compupdate off region 'us-east-1';
                """\
                if sql_command else sql_command 
        

    def execute(self, context : dict) -> None:
        """Every operator must have an execute function
        _____
        arg: 
            - context : contains information abou the task (the execution time, configuration, ...)
        """
        try:
            self.log.info("Copying data from S3 to Redshift")
            redshift_hook = PostgresHook("redshift")
            redshift_hook.run(self.sql_command)
            self.log.info("Finished Copying data from S3 to Redshift")
        except Exception as e:
            self.log.info(f"Error while Copying data from S3 to Redshift:\n{e}")
        
        





