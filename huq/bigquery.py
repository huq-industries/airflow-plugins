# -*- coding: utf-8 -*-
#
# Copyright 2019 Huq Industries.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.models import BaseOperator

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.decorators import apply_defaults
import time

"""
This module contains Google BigQuery and Google BigQuery to GCS Chains operators.
"""


# Will show up under airflow.operators.chains.BigQueryChainOperator
class BigQueryChainOperator(BaseOperator):
    """
    Executes a chain of BigQuery SQL queries in a specific BigQuery database

    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a list of str (sql statements), or reference to a template files.
        Template reference are recognized by str ending in '.sql'.
    :param destination_dataset_table: A list of dotted
        (<project>.|<project>:)<dataset>.<table> strings that, if set, will store the results
        of the query. (templated)
    :type destination_dataset_table: list
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: string
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: string
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: boolean
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :type flatten_results: boolean
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: boolean
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: integer
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :type maximum_bytes_billed: float
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: tuple
    :param query_params: a dictionary containing query parameter types and
        values, passed to BigQuery.
    :type query_params: dict
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :type priority: string
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :type time_partitioning: dict
    :param: lazy_retry_wait: number of seconds to wait before retried a failed chained operation
        Default value is 10 seconds.
    :type: lazy_retry_wait: int
    """

    template_fields = ('sql', 'destination_dataset_table', 'labels')
    template_ext = ('.sql', )
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 sql=None,
                 destination_dataset_table=None,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 flatten_results=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 udf_config=False,
                 use_legacy_sql=True,
                 maximum_billing_tier=None,
                 maximum_bytes_billed=None,
                 create_disposition='CREATE_IF_NEEDED',
                 schema_update_options=(),
                 query_params=None,
                 labels=None,
                 priority='INTERACTIVE',
                 time_partitioning={},
                 lazy_retry_wait=10,
                 *args,
                 **kwargs):
        super(BigQueryChainOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.bq_cursor = None
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.lazy_retry_wait = lazy_retry_wait

        if self.sql is None:
            raise TypeError('{} missing 1 required positional '
                            'argument: `sql`'.format(self.task_id))
        elif isinstance(sql, str):
            self.sql = [sql]
        elif not isinstance(sql, list):
            raise TypeError('{} `sql` parameter type needs to be str or list'.format(self.task_id))

        if self.destination_dataset_table is None:
            raise TypeError('{} missing 1 required positional '
                            'argument: `destination_dataset_table`'.format(self.task_id))
        elif isinstance(destination_dataset_table, str):
            self.destination_dataset_table = [destination_dataset_table]
        elif not isinstance(destination_dataset_table, list):
            raise TypeError('{} `destination_dataset_table` parameter type '
                            'needs to be str or list'.format(self.task_id))

        if len(self.sql) != len(self.destination_dataset_table):
            raise ValueError('{} `sql` and `destination_dataset_table` '
                             'need to have the same length'.format(self.task_id))

    def execute(self, context):
        if self.bq_cursor is None:
            hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to)
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
        for i in range(0, len(self.sql), 1):
            try:
                self.log.info('Executing %d/%d queries', i+1, len(self.sql))
                self.log.info('Executing: %s', self.sql[i])
                self.bq_cursor.run_query(
                    self.sql[i],
                    destination_dataset_table=self.destination_dataset_table[i],
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning
                )
            except Exception as e:
                self.log.error('Exception: %s', e)
                self.log.info('Wait %d seconds and retry', self.lazy_retry_wait)
                time.sleep(self.lazy_retry_wait)
                self.bq_cursor.run_query(
                    self.sql[i],
                    destination_dataset_table=self.destination_dataset_table[i],
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning
                )

    def on_kill(self):
        super(BigQueryChainOperator, self).on_kill()
        if self.bq_cursor is not None:
            self.log.info('Canceling running query due to execution timeout')
            self.bq_cursor.cancel_query()


# Will show up under airflow.operators.chains.BigQueryToCloudStorageChainOperator
class BigQueryToCloudStorageChainOperator(BaseOperator):
    """
    Chain multiple BigQuery table transfers to Google Cloud Storage.

    .. see also::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    :param source_project_dataset_table: A list of dotted
        (<project>.|<project>:)<dataset>.<table> BigQuery table to use as the source
        data. If <project> is not included, project will be the project
        defined in the connection json. (templated)
    :type source_project_dataset_tables: list
    :param destination_cloud_storage_uris: A list of destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :type destination_cloud_storage_uris: list
    :param compression: Type of compression to use.
    :type compression: string
    :param export_format: File format to export.
    :type field_delimiter: string
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :type field_delimiter: string
    :param print_header: Whether to print a header for a CSV file extract.
    :type print_header: boolean
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param: lazy_retry_wait: number of seconds to wait before retried a failed chained operation
        Default value is 10 seconds.
    :type: lazy_retry_wait: int
    """
    template_fields = ('source_project_dataset_table',
                       'destination_cloud_storage_uris', 'labels')
    template_ext = ('.sql',)
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(self,
                 source_project_dataset_table,
                 destination_cloud_storage_uris,
                 compression='NONE',
                 export_format='CSV',
                 field_delimiter=',',
                 print_header=True,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 labels=None,
                 lazy_retry_wait=10,
                 *args,
                 **kwargs):
        super(BigQueryToCloudStorageChainOperator, self).__init__(*args, **kwargs)
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.lazy_retry_wait = lazy_retry_wait

        if self.source_project_dataset_table is None:
            raise TypeError('{} missing 1 required positional '
                            'argument: `source_project_dataset_table`'.format(self.task_id))
        elif isinstance(source_project_dataset_table, str):
            self.source_project_dataset_table = [source_project_dataset_table]
        elif not isinstance(source_project_dataset_table, list):
            raise TypeError('{} `source_project_dataset_table` parameter type needs to be str or list'.format(self.task_id))

        if self.destination_cloud_storage_uris is None:
            raise TypeError('{} missing 1 required positional '
                            'argument: `destination_cloud_storage_uris`'.format(self.task_id))
        elif isinstance(destination_cloud_storage_uris, str):
            self.destination_cloud_storage_uris = [destination_cloud_storage_uris]
        elif not isinstance(destination_cloud_storage_uris, list):
            raise TypeError('{} `destination_cloud_storage_uris`parameter type '
                            'needs to be str or list'.format(self.task_id))

        if len(self.source_project_dataset_table) != len(self.destination_cloud_storage_uris):
            raise ValueError('{} `source_project_dataset_table` and `destination_cloud_storage_uris` '
                             'need to have the same length'.format(self.task_id))

    def execute(self, context):
        for i in range(0, len(self.source_project_dataset_table), 1):
            try:
                self.log.info('Executing %d/%d extracts', i+1, len(self.source_project_dataset_table))
                self.log.info('Executing extract of %s into: %s',
                              self.source_project_dataset_table[i],
                              self.destination_cloud_storage_uris[i])
                hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                                    delegate_to=self.delegate_to)
                conn = hook.get_conn()
                cursor = conn.cursor()
                cursor.run_extract(
                    self.source_project_dataset_table[i],
                    self.destination_cloud_storage_uris[i],
                    self.compression,
                    self.export_format,
                    self.field_delimiter,
                    self.print_header,
                    self.labels)
            except Exception as e:
                self.log.error('Exception: %s', e)
                self.log.info('Wait %d seconds retry', self.lazy_retry_wait)
                time.sleep(self.lazy_retry_wait)
                hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                                    delegate_to=self.delegate_to)
                conn = hook.get_conn()
                cursor = conn.cursor()
                cursor.run_extract(
                    self.source_project_dataset_table[i],
                    self.destination_cloud_storage_uris[i],
                    self.compression,
                    self.export_format,
                    self.field_delimiter,
                    self.print_header,
                    self.labels)
