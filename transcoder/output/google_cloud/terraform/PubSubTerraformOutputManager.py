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
from transcoder.output.google_cloud.Constants import GOOGLE_PACKAGED_SOLUTION_KEY, GOOGLE_PACKAGED_SOLUTION_VALUE
from transcoder.output.google_cloud.terraform.GcpTerraformOutputManager import GCPTerraformOutputManager


class PubSubTerraformOutputManager(GCPTerraformOutputManager):
    """Output manager for representing schemas as PubSub topic and schema terraform resources"""

    @staticmethod
    def output_type_identifier():
        return 'pubsub_terraform'

    def __init__(self, project_id: str, output_encoding: str,
                 create_schema_enforcing_topics: bool = True, output_path: str = None):
        super().__init__('pubsub', output_path)
        self.project_id = project_id
        self.is_binary_encoded = output_encoding.lower() == "binary"
        self.create_schema_enforcing_topics = create_schema_enforcing_topics

    def _create_field(self, field: DatacastField):
        return field.create_avro_field()

    def _add_schema(self, schema: DatacastSchema):
        _fields = self._get_field_list(schema.fields)
        schema_dict = {'type': 'record', 'namespace': 'sbeMessage', 'name': schema.name, 'fields': _fields}
        schema_json = json.dumps(schema_dict)  # .replace('"', '\\"')
        schema_json = json.dumps(schema_json)

        content = ""
        if self.create_schema_enforcing_topics is True:
            content += f"""resource \"google_pubsub_schema\" \"{schema.name}\" {{
  name = \"{schema.name}\"
  type = \"AVRO\"
  definition = {schema_json}
}}

"""

            content += f"""resource \"google_pubsub_topic\" \"{schema.name}\" {{
  name = \"{schema.name}\"

  schema_settings {{
    schema = \"projects/{self.project_id}/schemas/{schema.name}\"
    encoding = \"{'BINARY' if self.is_binary_encoded is True else 'JSON'}\"
  }}
  
  labels = {{
    \"{GOOGLE_PACKAGED_SOLUTION_KEY}\" = \"{GOOGLE_PACKAGED_SOLUTION_VALUE}\"
  }}
  
  depends_on = [google_pubsub_schema.{schema.name}]
}}
"""

        self._save_schema(schema.name, content)
