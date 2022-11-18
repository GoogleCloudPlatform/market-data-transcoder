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

#from genson import SchemaBuilder
from transcoder.message import DatacastSchema, DatacastField
from transcoder.output import OutputManager

class JsonOutputManager(OutputManager):
    """Transcode messages to JSON encoding persisted to a file"""
    def __init__(self, prefix: str, output_path: str, lazy_create_resources: bool = False):
        super().__init__(lazy_create_resources=lazy_create_resources)
        self.prefix = prefix
        self.schemas = {}
        self.writers = {}

        if output_path is None:
            rel_path = "jsonOut"
            main_script_dir = os.path.dirname(sys.argv[0])
            self.output_path = os.path.join(main_script_dir, rel_path)
        else:
            self.output_path = output_path

        exists = os.path.exists(self.output_path)
        if not exists:
            os.makedirs(self.output_path)

    @staticmethod
    def init_schema():
        schema = {}
        schema['$schema'] = 'https://json-schema.org/draft/2019-09/schema'
        schema['type'] = 'object'
        return schema

    @staticmethod
    def output_type_identifier():
        return 'json'

    def _create_field(self, field: DatacastField):
        return field.create_json_field()

    def _add_schema(self, schema: DatacastSchema):

        if schema.name in self.schemas:
            del self.schemas[schema.name]
        if schema.name in self.writers:
            self.writers[schema.name].close()
            del self.writers[schema.name]

        output_file = open(self._get_file_name(schema.name, 'json'), 'w')  # pylint: disable=consider-using-with
        output_file.truncate()

        schema_json = JsonOutputManager.init_schema()
        schema_json['name'] = schema.name
        schema_json['properties'] = {}

        for field in schema.fields:
            json_field = field.create_json_field()
            if json_field['definition']['type'] == 'array':
                schema_json['properties'][field.name] = json_field['properties'][0]['definition']
            else:
                schema_json['properties'][field.name] = json_field['definition']

        obj = json.dumps(schema_json)
        self._save_schema(schema.name, obj)
        self.schemas[schema.name] = obj
        self.writers[schema.name] = output_file

    def _write_record(self, record_type_name, record):
        self.writers[record_type_name].write(json.dumps(record) + '\n')

    def _save_schema(self, name, schema_json):
        with open(self.get_schema_file_name(name, 'json'), mode='wt', encoding='utf-8') as file:
            file.write(schema_json)

    def get_schema_file_name(self, name, extension):
        return self.output_path + '/' + self.prefix + '-' + name + '.schema.' + extension

    def _get_file_name(self, name, extension):
        return self.output_path + '/' + self.prefix + '-' + name + '.' + extension

    def wait_for_completion(self):
        super().wait_for_completion()
        for _, writer in self.writers.items():
            writer.close()
