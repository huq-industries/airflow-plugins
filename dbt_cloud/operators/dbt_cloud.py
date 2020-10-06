# -*- coding: utf-8 -*-
#
# Copyright 2020 Huq Industries.
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
from dbt_cloud.hooks.dbt_cloud import DbtCloudHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


def dbt_cloud_job_factory(job_specs, **kwargs):
    """
    Update keywords in job_specs with values that can be templated
    """
    job = job_specs
    job["id"] = None
    job["state"] = 1
    job["generate_docs"] = True
    for key, val in kwargs.items():
        job[key] = val
    return job


class DbtCloudCreateJobOperator(BaseOperator):
    """
    Create new dbt cloud job.
    """
    template_fields = (
        'project_id', 'environment_id', 'name',
        'dbt_version', 'execute_steps'
    )

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 account_id, project_id, environment_id, name,
                 dbt_version, execute_steps,
                 job_specs,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id

        self.account_id = account_id
        self.project_id = project_id
        self.environment_id = environment_id
        self.name = name
        self.dbt_version = dbt_version
        self.execute_steps = execute_steps

        self.job_specs = job_specs
        self.job = None

    def execute(self, context):
        self.job = dbt_cloud_job_factory(
            job_specs=self.job_specs,
            account_id=self.account_id,
            project_id=self.project_id,
            environment_id=self.environment_id,
            name=self.name,
            dbt_version=self.dbt_version,
            execute_steps=self.execute_steps,
        )

        self.log.info(f'Trying to create the dbt cloud job: {self.job}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.create_job(self.job)
            job = resp.get('job')
            self.log.info(f'Created job ID {job.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error creating job: {self.job}: {e}')

        return job.get('id')


class DbtCloudUpdateJobOperator(BaseOperator):
    """
    Updating existing dbt cloud job.
    """
    template_fields = (
        'job_id', 'project_id', 'environment_id', 'name',
        'dbt_version', 'execute_steps'
    )

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 job_id, account_id, project_id, environment_id, name,
                 dbt_version, execute_steps,
                 job_specs,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        if job_id is None:
            raise AirflowException('Job ID not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_id = job_id
        self.account_id = account_id
        self.project_id = project_id
        self.environment_id = environment_id
        self.name = name
        self.dbt_version = dbt_version
        self.execute_steps = execute_steps

        self.job_specs = job_specs
        self.job = None

    def execute(self, context):

        self.job = dbt_cloud_job_factory(
            job_specs=self.job_specs,
            id=self.job_id,
            account_id=self.account_id,
            project_id=self.project_id,
            environment_id=self.environment_id,
            name=self.name,
            dbt_version=self.dbt_version,
            execute_steps=self.execute_steps,
        )

        self.log.info(f'Trying to update the dbt cloud job: {self.job_id}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.update_job(self.job)
            job = resp.get('job')
            self.log.info(f'Updated job {job.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error updating job {self.job_id}: {e}')

        return job.get('id')


class DbtCloudDeleteJobOperator(BaseOperator):
    """
    Delete dbt cloud job

    We use job_id as either job_id or a task_id that return a job_id.
    """

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 job_id,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        if job_id is None:
            raise AirflowException('Job ID not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_id = job_id

    def execute(self, context):

        self.log.info(f'Trying to delete the dbt cloud job: {self.job_id}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.delete_job(self.job_id)
            job = resp.get('job')
            self.log.info(f'Deleted job {job.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error deleting job {self.job_id}: {e}')

        return job.get('id')


class DbtCloudTriggerRunOperator(BaseOperator):
    """
    Trigger a run for a dbt cloud job.

    We use job_id as either job_id or a task_id that return a job_id.
    """
    template_fields = ('job_id',)

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 job_id,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        if job_id is None:
            raise AirflowException('Job ID not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_id = job_id

    def execute(self, context):

        self.log.info(f'Trigger a run for the dbt cloud job: {self.job_id}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.trigger_run(self.job_id)
            run = resp.get('run')
            self.log.info(f'Triggered job {self.job_id} | run {run.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error triggering job {self.job_id}: {e}')

        return run.get('id')


class DbtCloudUpdateJobAndTriggerRunOperator(BaseOperator):
    """
    Update and run a dbt cloud job
    """

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 job_id, project_id, environment_id, name, dbt_version,
                 execute_steps,
                 job_specs,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        if job_id is None:
            raise AirflowException('Job ID not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.job_id = job_id
        self.job_specs = job_specs

    def execute(self, context):

        self.job = dbt_cloud_job_factory(
            job_specs=self.job_specs,
            id=self.job_id,
            account_id=self.account_id,
            project_id=self.project_id,
            environment_id=self.environment_id,
            name=self.name,
            dbt_version=self.dbt_version,
            execute_steps=self.execute_steps,
        )

        self.log.info(f'Trigger a run for the dbt cloud job: {self.job_id}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.update_job(self.job)
            job = resp.get('job')
            self.log.info(f'Updated job {job.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error updating job {self.job_id}: {e}')

        try:
            dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
            resp = dbt_cloud_hook.trigger_run(self.job_id)
            run = resp.get('run')
            self.log.info(f'Triggered job {self.job_id} | run {run.get("id")}')
        except RuntimeError as e:
            raise AirflowException(f'Error triggering job {self.job_id}: {e}')

        return run.get('id')
