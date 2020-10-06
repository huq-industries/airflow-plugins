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

import dbt_cloud_client
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class RunStatus:
    STATUS = {
        1: 'Queued',
        2: 'Queued',
        3: 'Running',
        10: 'Success',
        20: 'Error',
        30: 'Cancelled',
    }

    @classmethod
    def get_status_name(cls, state):
        return cls.STATUS.get(state, 'Unknown')


class DbtCloudHook(BaseHook):
    """
    Interact with dbt Cloud.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    """
    def __init__(self, dbt_cloud_conn_id):
        self.conn_id = dbt_cloud_conn_id
        self.account_id = None
        self.api_token = None
        extra = self.get_connection(self.conn_id).extra_dejson

        if 'account_id' in extra:
            self.account_id = extra.get('account_id')
        else:
            raise AirflowException('dbt Cloud Account ID not found.')

        if 'api_token' in extra:
            self.api_token = extra.get('api_token')
        else:
            raise AirflowException('dbt Cloud API Token not found.')

        self.client = dbt_cloud_client.ApiClient(
            configuration=None,
            header_name='Authorization',
            header_value=f'Token {self.api_token}'
        )

        self.accounts_client = dbt_cloud_client.AccountsApi(self.client)
        self.connections_client = dbt_cloud_client.ConnectionsApi(self.client)
        self.credentials_client = dbt_cloud_client.CredentialsApi(self.client)
        self.environments_client = dbt_cloud_client.EnvironmentsApi(self.client)  # noqa: E501
        self.jobs_client = dbt_cloud_client.JobsApi(self.client)
        self.repository_client = dbt_cloud_client.RepositoriesApi(self.client)
        self.runs_client = dbt_cloud_client.RunsApi(self.client)

    def create_job(self, job_data):
        """
        Create a Job
        :param job_data: Dictionary containing the job configuration.
            https://docs.getdbt.com/dbt-cloud/api/#operation/createJob
        :type job_data: dict.
        """
        resp = self.jobs_client.create_job(self.account_id, body=job_data)
        return {
            'job': resp.data.to_dict(),
            'status': resp.status.to_dict(),
        }

    def update_job(self, job_data):
        """
        Update an existing job
        :param job_data: Dictionary containing the job configuration.
            https://docs.getdbt.com/dbt-cloud/api/#operation/updateJobById
        :type job_data: dict.
        """
        if job_data.get('job_id') is not None:
            raise AirflowException('"job_id" is required to update a job')
        resp = self.jobs_client.update_job_by_id(
                account_id=self.account_id,
                job_id=job_data.get('id'),
                body=job_data,
            )
        return {
            'job': resp.data.to_dict(),
            'status': resp.status.to_dict(),
        }

    def delete_job(self, job_id):
        """
        Delete a job
        :param job_id: Numeric ID of the job to delete.
            https://docs.getdbt.com/dbt-cloud/api/#operation/deleteJobById
        :type job_id: int.

        """
        resp = self.jobs_client.delete_job_by_id(self.account_id, job_id=job_id)  # noqa: E501
        return {
            'job': resp.data.to_dict(),
            'status': resp.status.to_dict(),
        }

    def trigger_run(self, job_id, trigger_data={'cause': 'Airflow Trigger'}):
        """
        Trigger a job run
        :param job_id: Numeric ID of the job to trigger.
            https://docs.getdbt.com/dbt-cloud/api/#operation/triggerRun
        :type job_id: int.
        :parma trigger_data: Dictionary with trigger information.
        :type trigger_data: dict
        """
        resp = self.jobs_client.trigger_run(
                            self.account_id,
                            job_id=job_id,
                            body=trigger_data,
                        )
        return {
            'run': resp.data.to_dict(),
            'status': resp.status.to_dict(),
        }

    def cancel_run(self, run_id):
        """
        Cancel a run
        :param run_id: Numeric ID of the run to cancel.
            https://docs.getdbt.com/dbt-cloud/api/#operation/cancelRunById
        :type run_id: int
        """
        resp = self.runs_client.cancel_run_by_id(self.account_id, run_id=run_id)  # noqa: E501
        return {
            'run': resp.data.to_dict(),
            'status': resp.status.to_dict(),
        }

    def get_run_status_code(self, run_id):
        """
        Return the status of an dbt cloud run.
        :param run_id: Numeric ID of the run to cancel.
            https://docs.getdbt.com/dbt-cloud/api/#operation/getRunById
        :type run_id: int
        """
        resp = self.runs_client.get_run_by_id(self.account_id, run_id=run_id)
        return resp.data.to_dict().get('status')

    def get_run_status_name(self, run_id):
        """
        Return the status name of an dbt cloud run.
        :param run_id: Numeric ID of the run to cancel.
            https://docs.getdbt.com/dbt-cloud/api/#operation/getRunById
        :type run_id: int
        """
        return RunStatus.get_status_name(self.get_run_status_code(run_id))
