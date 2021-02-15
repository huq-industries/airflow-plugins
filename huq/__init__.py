from airflow.plugins_manager import AirflowPlugin

from huq.gcs import (
    GoogleCloudStorageComposePrefixOperator,
    GoogleCloudStorageToS3CopyObjectListOperator,
    GoogleCloudStorageToS3CopyOperator,
    GoogleCloudStorageToS3CopyPrefixOperator,
)


# Defining the plugin class
class AirflowHuqPlugin(AirflowPlugin):
    name = "huq"
    operators = [
        GoogleCloudStorageComposePrefixOperator,
        GoogleCloudStorageToS3CopyObjectListOperator,
        GoogleCloudStorageToS3CopyOperator,
        GoogleCloudStorageToS3CopyPrefixOperator,
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
