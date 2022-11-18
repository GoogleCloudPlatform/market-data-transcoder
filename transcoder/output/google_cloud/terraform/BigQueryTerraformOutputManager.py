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

from transcoder.message import DatacastSchema, DatacastField
from transcoder.output.google_cloud.Constants import GOOGLE_PACKAGED_SOLUTION_VALUE, GOOGLE_PACKAGED_SOLUTION_KEY
from transcoder.output.google_cloud.terraform.GcpTerraformOutputManager import GCPTerraformOutputManager


class BigQueryTerraformOutputManager(GCPTerraformOutputManager):
    """Output manager for representing schemas as BigQuery table terraform resources"""

    @staticmethod
    def output_type_identifier():
        return 'bigquery_terraform'

    def __init__(self, project_id: str, dataset_id, output_path: str = None):
        super().__init__('bigquery', output_path)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def _create_field(self, field: DatacastField):
        return field.create_bigquery_field()

    def _add_schema(self, schema: DatacastSchema):
        _fields = self._get_field_list(schema.fields)
        schema_str = BigQueryTerraformOutputManager._generate_schema(_fields)
        schema_str_json = json.dumps(schema_str, indent=2)
        schema_str_json_indented = GCPTerraformOutputManager.reindent(schema_str_json, 4)

        content = f"""resource \"google_bigquery_table\" \"{schema.name}\" {{
  dataset_id = \"{self.dataset_id}\"
  table_id   = \"{schema.name}\"

  labels = {{
    \"{GOOGLE_PACKAGED_SOLUTION_KEY}\" = \"{GOOGLE_PACKAGED_SOLUTION_VALUE}\"
  }}
  
  schema = <<EOF
{schema_str_json_indented}
  EOF
}}
"""
        self._save_schema(schema.name, content)

    @staticmethod
    def _generate_schema(fields):
        field_list = []
        for field in fields:
            _dict = {
                'name': field.name,
                'type': field.field_type,
                'mode': field.mode
            }
            if len(field.fields) > 0:
                _dict['fields'] = BigQueryTerraformOutputManager._generate_schema(field.fields)
            field_list.append(_dict)
        return field_list
