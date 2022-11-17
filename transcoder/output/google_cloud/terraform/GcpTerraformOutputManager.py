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
import os
import sys

from transcoder.message import DatacastField
from transcoder.output import OutputManager


class GCPTerraformOutputManager(OutputManager):
    """Output manager for representing schemas as terraform resources"""

    def __init__(self, prefix: str, output_path: str):
        super().__init__()
        self.prefix = prefix
        self.schemas = {}
        self.writers = {}

        if output_path is None:
            rel_path = "tfOut"
            main_script_dir = os.path.dirname(sys.argv[0])
            self.output_path = os.path.join(main_script_dir, rel_path)
        else:
            self.output_path = output_path

        exists = os.path.exists(self.output_path)
        if not exists:
            os.makedirs(self.output_path)

    def _create_field(self, field: DatacastField):
        return field.create_bigquery_field()

    def write_record(self, record_type_name, record):
        pass

    def _save_schema(self, name, content):
        with open(self._get_file_name(name, 'tf'), mode='wt', encoding='utf-8') as file:
            file.write(content)

    def _get_file_name(self, name, extension):
        return self.output_path + '/' + self.prefix + '-' + name + '.' + extension

    def wait_for_completion(self):
        super().wait_for_completion()
        for _, writer in self.writers.items():
            writer.close()

    @staticmethod
    def reindent(s, num_spaces):
        s = s.split('\n')
        s = [(num_spaces * ' ') + line for line in s]
        s = '\n'.join(s)
        return s
