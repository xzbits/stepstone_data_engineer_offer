from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WranglingDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, process_wrangling, *args, **kwargs):
        super(WranglingDataOperator, self).__init__(*args, **kwargs)
        self.process_wrangling = process_wrangling

    def execute(self, context):
        self.log.info('Starting WranglingDataOperator for all tables ...')
        self.process_wrangling()
        self.log.info('Finish WranglingDataOperator!')
