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

from dbt_cloud.hooks.dbt_cloud import DbtCloudHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DbtCloudRunSensor(BaseSensorOperator):
    """
    Asks for the state of a dbt Cloud run until it reaches a terminal state.
    If it fails the sensor errors, failing the task.
    :param dbt_cloud_conn_id: dbt Cloud connection ID.
    :type dbt_cloud_conn_id: string
    :param run_id: dbt cloud run ID.
    :type run_id: int
    """
    template_fields = ('run_id',)
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 run_id,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if dbt_cloud_conn_id is None:
            raise AirflowException('dbt Cloud connection id not found.')

        if run_id is None:
            raise AirflowException('Run ID not found.')

        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.run_id = run_id

        self.terminal_states = ['Success', 'Error', 'Cancelled']
        self.failed_states = ['Error']

    def poke(self, context):
        self.log.info(f'Sensor checking the state of dbt cloud run ID: {self.run_id}')  # noqa: E501
        dbt_cloud_hook = DbtCloudHook(self.dbt_cloud_conn_id)
        run_status = dbt_cloud_hook.get_run_status_name(self.run_id)
        self.log.info(f'State of Run ID {self.run_id}: {run_status}')

        if run_status in self.failed_states:
            raise AirflowException(f'dbt cloud Run ID {self.run_id} Failed.')
        if run_status in self.terminal_states:
            return True
        else:
            return False
