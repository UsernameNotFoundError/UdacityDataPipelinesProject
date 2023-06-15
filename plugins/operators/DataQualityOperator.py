from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

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
        metastoreBackend = MetastoreBackend()
        self.aws_connection = metastoreBackend.get_connection(aws_credentials_id)
        self.tables_to_check = kwargs["params"]["tables_to_check"]

    def execute(self, context):
        try:
            for table in self.tables_to_check:
                self.log.info("Running data quality check")
                redshift_hook = PostgresHook("redshift")
                # Code from Udacity 
                ''' # Check the tables present 
                available_tables = redshift_hook.get_records(f"""SELECT table_name
                                                            FROM information_schema.tables
                                                            WHERE table_schema = 'public';
                                                            """)
                self.log.info( {record[0] for record in available_tables})
                '''
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            self.log.info("Finished data quality check.")
            
        except Exception as e:
            self.log.info(f"Error while checking data quality, \n{e}")
        
        