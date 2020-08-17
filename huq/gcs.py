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
        super(self).__init__(*args, **kwargs)
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
    template_fields = ('gcs_object', 's3_uri')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 gcs_bucket,
                 gcs_object,
                 s3_bucket,
                 s3_uri,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 dest_aws_conn_id=None,
                 dest_verify=None,
                 *args,
                 **kwargs):

        super(self).__init__(*args, **kwargs)
        self.gcs_bucket = gcs_bucket
        self.gcs_object = gcs_object
        self.s3_bucket = s3_bucket
        self.s3_uri = s3_uri
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
        if gcs_hook.exists(self.gcs_bucket, self.gcs_object) is False:
            self.log.warning('Skip object not found: gs://%s/%s', self.gcs_bucket, self.gcs_object)
            return
        self.log.info('Download gs://%s/%s', self.gcs_bucket, self.gcs_object)
        file_bytes = gcs_hook.download(
            self.gcs_bucket,
            self.gcs_object
        )
        self.log.info('Upload s3://%s/%s', self.s3_bucket, self.s3_uri)
        s3_hook.load_bytes(
            bytes_data=file_bytes,
            bucket_name=self.s3_bucket,
            key=self.s3_uri,
            replace=True,
        )


class GoogleCloudStorageToS3CopyChainOperator(BaseOperator):
    template_fields = ('gcs_source_objects', 's3_destination_uris')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 gcs_source_buckets,
                 gcs_source_objects,
                 s3_destination_buckets,
                 s3_destination_uris,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 dest_aws_conn_id=None,
                 dest_verify=None,
                 *args,
                 **kwargs):

        super(GoogleCloudStorageToS3CopyChainOperator, self).__init__(*args, **kwargs)
        self.gcs_source_buckets = gcs_source_buckets
        self.gcs_source_objects = gcs_source_objects
        self.s3_destination_buckets = s3_destination_buckets
        self.s3_destination_uris = s3_destination_uris
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
        for i in range(0, len(self.gcs_source_buckets), 1):
            if gcs_hook.exists(self.gcs_source_buckets[i], self.gcs_source_objects[i]) is False:
                self.log.warning('Skip object not found: gs://%s/%s', self.gcs_source_buckets[i], self.gcs_source_objects[i])
                continue
            self.log.info('Download gs://%s/%s', self.gcs_source_buckets[i], self.gcs_source_objects[i])
            file_bytes = gcs_hook.download(
                self.gcs_source_buckets[i],
                self.gcs_source_objects[i]
            )
            self.log.info('Upload s3://%s/%s', self.s3_destination_buckets[i], self.s3_destination_uris[i])
            s3_hook.load_bytes(
                bytes_data=file_bytes,
                bucket_name=self.s3_destination_buckets[i],
                key=self.s3_destination_uris[i],
                replace=True,
            )
