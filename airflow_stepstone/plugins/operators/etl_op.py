from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ETLOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, process_etl, *args, **kwargs):
        super(ETLOperator, self).__init__(*args, **kwargs)
        self.process_etl = process_etl
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id).get_conn()
        self.log.info('Starting ETLOperator ...')
        self.process_etl(postgres_hook)
        self.log.info('Finish ETLOperator!')
