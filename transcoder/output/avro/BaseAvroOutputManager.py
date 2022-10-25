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

import json
import os
import sys

from transcoder.message import DatacastField, DatacastSchema
from transcoder.output import OutputManager
from transcoder.output.exception import OutputFunctionNotDefinedError


class BaseAvroOutputManager(OutputManager):
    """Base avro output manager implementation. Used by both avro.io and fastavro implementations."""

    def __init__(self, prefix: str, output_path: str, lazy_create_resources: bool = False):
        super().__init__(lazy_create_resources=lazy_create_resources)
        self.prefix = prefix
        self.schemas = {}
        self.writers = {}

        if output_path is None:
            rel_path = "avroOut"
            main_script_dir = os.path.dirname(sys.argv[0])
            self.output_path = os.path.join(main_script_dir, rel_path)
        else:
            self.output_path = output_path

        exists = os.path.exists(self.output_path)
        if not exists:
            os.makedirs(self.output_path)

    def _create_field(self, field: DatacastField):
        return field.create_avro_field()

    def _add_schema(self, schema: DatacastSchema):
        _fields = self._get_field_list(schema.fields)
        if schema.name in self.schemas.keys():
            del self.schemas[schema.name]
        if schema.name in self.writers.keys():
            self.writers[schema.name].close()
            del self.writers[schema.name]

        schema_dict = {'type': 'record', 'namespace': 'sbeMessage', 'name': schema.name, 'fields': _fields}
        schema_json = json.dumps(schema_dict)
        self._save_schema(schema.name, schema_json)
        self.schemas[schema.name] = self._parse_schema(schema_dict)

    def _parse_schema(self, schema_dict):
        raise OutputFunctionNotDefinedError

    def _save_schema(self, name, schema_json):
        with open(self._get_file_name(name, 'avsc'), 'wt') as file:
            file.write(schema_json)

    def _get_file_name(self, name, extension):
        return self.output_path + '/' + self.prefix + '-' + name + '.' + extension

    def wait_for_completion(self):
        super().wait_for_completion()
        for writer_name in self.writers.keys():
            self.writers[writer_name].close()
