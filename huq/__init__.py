from airflow.plugins_manager import AirflowPlugin

from huq.bigquery import BigQueryChainOperator, BigQueryToCloudStorageChainOperator
from huq.gcs import GoogleCloudStorageComposePrefixOperator, GoogleCloudStorageComposePrefixChainOperator


# Defining the plugin class
class AirflowChainsPlugin(AirflowPlugin):
    name = "huq"
    operators = [
        BigQueryChainOperator,
        BigQueryToCloudStorageChainOperator,
        GoogleCloudStorageComposePrefixOperator,
        GoogleCloudStorageComposePrefixChainOperator,
    ]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
