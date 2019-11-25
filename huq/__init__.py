from airflow.plugins_manager import AirflowPlugin

from huq.bigquery import BigQueryChainOperator, BigQueryToCloudStorageChainOperator
from huq.gcs import GoogleCloudStorageComposePrefixOperator, GoogleCloudStorageComposePrefixChainOperator, GoogleCloudStorageToS3CopyChainOperator


# Defining the plugin class
class AirflowHuqPlugin(AirflowPlugin):
    name = "huq"
    operators = [
        BigQueryChainOperator,
        BigQueryToCloudStorageChainOperator,
        GoogleCloudStorageComposePrefixOperator,
        GoogleCloudStorageComposePrefixChainOperator,
        GoogleCloudStorageToS3CopyChainOperator,
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
