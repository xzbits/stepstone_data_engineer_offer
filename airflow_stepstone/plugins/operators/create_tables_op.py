from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, process_create_tables,
                 drop_tables_queries, create_tables_queries, *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.drop_queries = drop_tables_queries
        self.create_queries = create_tables_queries
        self.process_create_tables = process_create_tables
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id).get_conn()
        self.log.info('Starting CreateTablesOperator for all tables ...')
        self.process_create_tables(postgres_hook, self.drop_queries, self.create_queries)
        self.log.info('Finish CreateTablesOperator!')
