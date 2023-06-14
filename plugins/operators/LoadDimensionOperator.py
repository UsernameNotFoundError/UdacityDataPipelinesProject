from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Requires SQL to be executed 
    Used to transfer the data
    """
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(
            self,
            redshift_conn_id="",
            aws_credentials_id="",
            sql_command="",
            table_name="",
            *args, 
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_command = sql_command 
        self.table_name = table_name
        

    def execute(self, context : dict) -> None:
        """Every operator must have an execute function
        _____
        arg: 
            - context : contains information abou the task (the execution time, configuration, ...)
        """
        try:
            self.log.info(f"Initiating data loading in {self.table_name}")
            redshift_hook = PostgresHook("redshift")
            redshift_hook.run(self.sql_command)
            self.log.info(f"Successfully, Loaded data in {self.table_name}")
        except Exception as e:
            self.log.info(f"Error occured while loading data into\
                {self.table_name}:\n{e}")






