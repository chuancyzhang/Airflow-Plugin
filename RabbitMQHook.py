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

"""
RabbitMQHook module
"""
import pika

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class RabbitMQHook(BaseHook, LoggingMixin):
    """
    Hook to interact with RabbitMQ database
    """
    def __init__(self, rabbitmq_conn_id='rabbitmq_default'):
        """
        Prepares hook to connect to a RabbitMQ.
        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to RabbitMQ.
        """
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.client = None
        conn = self.get_connection(self.rabbitmq_conn_id)
        self.host = conn.host
        self.port = int(conn.port)
        self.password = conn.password

        self.log.debug(
            '''Connection "{conn}":
            \thost: {host}
            \tport: {port}
            \textra: {extra}
            '''.format(
                conn=self.rabbitmq_conn_id,
                host=self.host,
                port=self.port,
                extra=conn.extra_dejson
            )
        )

    def get_conn(self):
        """
        Returns a RabbitMQ connection.
        """
        if not self.client:
            self.log.debug(
                'generating RabbitMQ connection for conn_id "%s" on %s:%s:%s',
                self.rabbitmq_conn_id, self.host, self.port, self.db
            )
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    password=self.password))
            except Exception as general_error:
                raise AirflowException(
                    'Failed to create RabbitMQ connection, error: {error}'.format(
                        error=str(general_error)
                    )
                )

        return self.client

