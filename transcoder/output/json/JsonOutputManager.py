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
import datetime
import json

from transcoder.message import DatacastSchema, DatacastField
from transcoder.output import OutputManager


class JsonOutputManager(OutputManager):
    """Transcode messages to JSON encoding persisted to a file"""

    def __init__(self, prefix: str, output_path: str, lazy_create_resources: bool = False):
        super().__init__(lazy_create_resources=lazy_create_resources)
        self.prefix = prefix
        self.schemas = {}
        self.writers = {}
        self.output_path = self.create_output_path(output_path, 'jsonOut')

    @staticmethod
    def output_type_identifier():
        return 'jsonl'

    def _create_field(self, field: DatacastField):
        return field.create_json_field(field)

    def _add_schema(self, schema: DatacastSchema):
        # pylint: disable=duplicate-code
        if schema.name in self.schemas:
            del self.schemas[schema.name]
        if schema.name in self.writers:
            self.writers[schema.name].close()
            del self.writers[schema.name]

        output_file = open(  # pylint: disable=consider-using-with
            self._get_file_name(schema.name, 'jsonl'), 'w',
            encoding='utf-8')

        schema_json = {
            '$schema': 'https://json-schema.org/draft/2019-09/schema',
            'type': 'object',
            'name': schema.name,
            'properties': {}}

        for field in schema.fields:
            schema_json['properties'][field.name] = field.create_json_field(field)

        obj = json.dumps(schema_json)
        self._save_schema(schema.name, obj)
        self.schemas[schema.name] = obj
        self.writers[schema.name] = output_file

    def _write_record(self, record_type_name, record):
        self.writers[record_type_name].write(json.dumps(record, default=JsonOutputManager.default_formatter) + '\n')

    @staticmethod
    def default_formatter(obj):
        """Custom encoding to serialize additional types as needed"""
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return ''

    def _save_schema(self, name, schema_json):
        with open(self.get_schema_file_name(name, 'json'), mode='wt', encoding='utf-8') as file:
            file.write(schema_json)

    def get_schema_file_name(self, name, extension):
        """Returns a file name for the schema file"""
        return self.output_path + '/' + self.prefix + '-' + name + '.schema.' + extension

    def _get_file_name(self, name, extension):
        return self.output_path + '/' + self.prefix + '-' + name + '.' + extension

    def wait_for_completion(self):
        super().wait_for_completion()
        for _, writer in self.writers.items():
            writer.close()
