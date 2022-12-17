from airflow.plugins_manager import AirflowPlugin
from operators.wrangling_data_op import WranglingDataOperator
from operators.etl_op import ETLOperator
from operators.create_tables_op import CreateTablesOperator


class StepstonePlugin(AirflowPlugin):
    name = "stepstone_plugin"
    operators = [WranglingDataOperator, ETLOperator, CreateTablesOperator]
