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

from typing import Any

from google.cloud import bigquery

from transcoder.message.DatacastField import DatacastField


class DatacastGroup(DatacastField):
    """Implementation class encapsulating grouped fields"""

    def __init__(self, name):
        self.name = name
        self.fields: [] = []

    def append_field(self, field):
        """Append a field to this instance's fields list"""
        self.fields.append(field)

    def cast_value_to_type(self, value, field_type, is_nullable: bool = True) -> Any:
        return str(value)

    def create_json_field(self, part: DatacastField = None):
        field = self
        if part is not None:
            field = part

        group = {'type': 'array', 'title': field.name, 'properties': {}}
        for child_field in self.fields:
            group['properties'][child_field.name] = child_field.create_json_field(child_field)

        return group

    def create_avro_field(self, part: DatacastField = None):
        field = self
        if part is not None:
            field = part

        children = []
        for child_field in self.fields:
            children.append(child_field.create_avro_field())

        return {
            'name': field.name,
            'type': ['null', {
                'type': 'array',
                'items': {
                    'name': field.name,
                    'type': 'record',
                    'fields': children
                }
            }],
            'default': None
        }

    def create_bigquery_field(self, part: DatacastField = None):
        field = self
        if part is not None:
            field = part

        children = []
        for child_field in self.fields:
            children.append(child_field.create_bigquery_field())

        return bigquery.SchemaField(field.name, 'RECORD', mode="REPEATED", fields=children)

    def __repr__(self):
        return f'DatacastGroup(name: {self.name})'
