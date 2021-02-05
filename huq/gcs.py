# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException

import tempfile

GCS_COMPOSE_CHUNKS = 30


class GoogleCloudStorageComposePrefixOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.
    :param bucket: The Google cloud storage bucket where the object is. (templated)
    :type bucket: str
    :param source_objects_prefix: List of GCS objects to compose. (templated)
    :type source_objects_prefix: str
    :param destination_uri: Destination GCS object path. (templated)
    :type destination_uri: str
    :param num_retries: Number of retries for each compose action.
    :type num_retries: int
    :param delete_sources: Flag to enable source deletion after composition.
    :type num_retries: bool
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('bucket', 'source_objects_prefix', 'destination_uri',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 source_objects_prefix,
                 destination_uri,
                 compose_num_retries=5,
                 delete_sources=False,
                 clear_destination=True,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.source_objects_prefix = source_objects_prefix
        self.destination_uri = destination_uri
        self.compose_num_retries = compose_num_retries
        self.delete_sources = delete_sources
        self.clear_destination = clear_destination
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        self._compose(
            hook=hook,
            delete_sources=self.delete_sources,
            clear_destination=self.clear_destination
        )
        return

    def _compose(self, hook, delete_sources=False, clear_destination=True):
        bucket = self.bucket
        source_objects_prefix = self.source_objects_prefix
        destination_uri = self.destination_uri
        self.log.info(
            'Compose all object matching "%s" prefix into "%s"',
            source_objects_prefix,
            destination_uri
        )

        source_objects = hook.list(bucket=bucket, prefix=source_objects_prefix, maxResults=1000)
        if source_objects is None or len(source_objects) == 0:
            self.log.info('No objects found matching the prefix: "%s"', source_objects_prefix)
            self.log.warning('Skip: "%s"', destination_uri)
            return

        self.log.info('Number of object to compose: %d', len(source_objects))

        if clear_destination and hook.exists(bucket=bucket, object=destination_uri):
            self.log.info('Delete %s', destination_uri)
            hook.delete(bucket=bucket, object=destination_uri)

        for i in range(0, len(source_objects), GCS_COMPOSE_CHUNKS):
            self.log.info('Compose objects from %d to %d', i, i + GCS_COMPOSE_CHUNKS - 1)
            objects_to_compose = source_objects[i:i + GCS_COMPOSE_CHUNKS]
            # If we do more than one iteration we need to include destination_uri into source_objects
            if i >= GCS_COMPOSE_CHUNKS:
                objects_to_compose.append(destination_uri)
            hook.compose(
                bucket=bucket,
                source_objects=objects_to_compose,
                destination_object=destination_uri,
                num_retries=self.compose_num_retries
            )
        if delete_sources:
            for source_object in source_objects:
                hook.delete(bucket=bucket, object=source_object)
        return


class GoogleCloudStorageToS3CopyOperator(BaseOperator):
    template_fields = ('gcs_source_uri', 's3_destination_uri')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 gcs_source_bucket,
                 gcs_source_uri,
                 s3_destination_bucket,
                 s3_destination_uri=None,
                 s3_acl_policy=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 dest_aws_conn_id=None,
                 dest_verify=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.gcs_source_bucket = gcs_source_bucket
        self.gcs_source_uri = gcs_source_uri
        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_uri = s3_destination_uri if s3_destination_uri is not None else self.gcs_source_uri  # noqa: E501
        self.s3_acl_policy = s3_acl_policy
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.delegate_to = delegate_to

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        s3_hook = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)
        if gcs_hook.exists(self.gcs_source_bucket, self.gcs_source_uri) is False:
            self.log.error('Skip object not found: gs://%s/%s', self.gcs_source_bucket, self.gcs_source_uri)
            raise AirflowException('Skip object not found: gs://%s/%s', self.gcs_source_bucket, self.gcs_source_uri)
        tmp = tempfile.NamedTemporaryFile()
        self.log.info('Download gs://%s/%s', self.gcs_source_bucket, self.gcs_source_uri)
        gcs_hook.download(
            bucket=self.gcs_source_bucket,
            object=self.gcs_source_uri,
            filename=tmp.name,
        )
        self.log.info('Upload s3://%s/%s', self.s3_destination_bucket, self.s3_destination_uri)
        s3_hook.load_file(
                filename=tmp.name,
            bucket_name=self.s3_destination_bucket,
            key=self.s3_destination_uri,
            replace=True,
            acl_policy=self.s3_acl_policy
        )
        tmp.close()


class GoogleCloudStorageToS3CopyObjectListOperator(BaseOperator):
    template_fields = ('gcs_source_uris', 's3_destination_uris')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 gcs_source_bucket,
                 gcs_source_uris,
                 s3_destination_bucket,
                 s3_destination_uris=None,
                 s3_acl_policy=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 dest_aws_conn_id=None,
                 dest_verify=None,
                 fail_on_missing=False,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.gcs_source_bucket = gcs_source_bucket
        self.gcs_source_uris = gcs_source_uris
        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_uris = s3_destination_uris if s3_destination_uris is not None else self.gcs_source_uris  # noqa: E501
        self.s3_acl_policy = s3_acl_policy
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.delegate_to = delegate_to
        self.fail_on_missing = fail_on_missing
        self.is_failed = False

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        s3_hook = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        for i in range(0, len(self.gcs_source_uris), 1):
            tmp = tempfile.NamedTemporaryFile()
            gcs_obj = self.gcs_source_uris[i]
            s3_obj = self.s3_destination_uris[i]
            if gcs_hook.exists(self.gcs_source_bucket, gcs_obj) is False:
                if self.fail_on_missing is True:
                    self.log.error('Execution will fail Object not found: gs://%s/%s', self.gcs_source_bucket, gcs_obj)
                    self.is_failed = True
                else:
                    self.log.warning('Skipping. Object not found: gs://%s/%s', self.gcs_source_bucket, gcs_obj)
                continue

            self.log.info('Download gs://%s/%s', self.gcs_source_bucket, gcs_obj)
            gcs_hook.download(
                bucket=self.gcs_source_bucket,
                object=gcs_obj,
                filename=tmp.name
            )
            self.log.info('Upload s3://%s/%s', self.s3_destination_bucket, s3_obj)
            s3_hook.load_file(
                filename=tmp.name,
                bucket_name=self.s3_destination_bucket,
                key=s3_obj,
                replace=True,
                acl_policy=self.s3_acl_policy
            )
            tmp.close()
            if self.is_failed:
                raise AirflowException('Some object were not found at the source.')
