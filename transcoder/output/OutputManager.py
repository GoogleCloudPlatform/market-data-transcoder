#
# Copyright 2022 Google LLC
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import concurrent
import os
import sys
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from transcoder.message import DatacastField, DatacastSchema
from transcoder.output.exception import OutputFunctionNotDefinedError, OutputManagerSchemaError


class OutputManager:
    """Abstract output manager class"""

    @staticmethod
    def output_type_identifier():
        """Returns identifier of output type"""
        raise OutputFunctionNotDefinedError

    @staticmethod
    def supports_data_writing():
        """Returns flag indicating if data writes are supported"""
        return True

    def __init__(self, schema_max_workers=5, lazy_create_resources: bool = False):
        self.schema_thread_pool_executor: ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(
            max_workers=schema_max_workers)
        self.lazy_create_resources = lazy_create_resources
        self.existing_schemas = {}
        self.schema_definitions = {}
        self.schema_futures = []

    def _create_field(self, field: DatacastField):
        raise OutputFunctionNotDefinedError

    def _get_field_list(self, fields: [DatacastField]):
        return list(map(self._create_field, fields))

    def enqueue_schema(self, schema):
        """Enqueues a schema for creation. If lazy_create_resources is enabled, schemas will only be created
        on-demand when required """
        if self.lazy_create_resources is False:
            future = self.schema_thread_pool_executor.submit(self.add_schema, schema)
            self.schema_futures.append(future)

        self.schema_definitions[schema.name] = schema

    def add_schema(self, schema: DatacastSchema):
        """Adds a DatacastSchema instance to list of known message schemas"""
        self._add_schema(schema)
        if schema.name not in self.existing_schemas:
            self.existing_schemas[schema.name] = True

    def _add_schema(self, schema: DatacastSchema):
        pass

    def write_record(self, record_type_name, record):
        """For record of given type optionally lazily creation resources and write record"""
        if self.lazy_create_resources is True and record_type_name not in self.existing_schemas:
            schema = self.schema_definitions[record_type_name]
            self.add_schema(schema)
        self._write_record(record_type_name, record)

    def _write_record(self, record_type_name, record):
        raise OutputFunctionNotDefinedError

    def wait_for_schema_creation(self):
        """Wait for enqueued schema resources. Nothing to wait for if lazy_create_resources is enabled."""
        self.schema_thread_pool_executor.shutdown(wait=True)
        result = futures.wait(self.schema_futures)
        exceptions = []
        for future in result.done:
            exception = future.exception()
            if exception:
                exceptions.append(exception)
        if len(exceptions) > 0:
            raise OutputManagerSchemaError(exceptions)

    def wait_for_completion(self):
        """Extend or override to wait until output manager has fully completed writing and other work"""
        self.wait_for_schema_creation()

    def create_output_path(self, output_path: str, relative_path: str):
        """Creates the output path if it doesn't exist. Output path will be created as {output_path}/{relative_path}"""
        _output_path = None
        if output_path is None:
            main_script_dir = os.path.dirname(sys.argv[0])
            _output_path = os.path.join(main_script_dir, relative_path)
        else:
            _output_path = output_path
        exists = os.path.exists(_output_path)
        if not exists:
            os.makedirs(_output_path)
        return _output_path
