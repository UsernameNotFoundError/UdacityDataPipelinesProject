from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            aws_credentials_id="",
            *args, 
            **kwargs
        ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        try:
            with open("create_tables.sql", "r") as sql_file:
                self.log.info("Creating tables in Redshift")
                redshift_hook = PostgresHook("redshift")
                redshift_hook.run(sql_file.read())
                self.log.info("Finished initiating tables in Redshift")
                
        except Exception as e:
            self.log.info(f"Error initiating tables:\n{e}")
