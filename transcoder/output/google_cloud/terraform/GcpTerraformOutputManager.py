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

from transcoder.output import OutputManager


class GCPTerraformOutputManager(OutputManager):
    """Output manager for representing schemas as terraform resources"""

    @staticmethod
    def supports_data_writing():
        """Returns flag indicating if data writes are supported"""
        return False

    def __init__(self, prefix: str, output_path: str):
        super().__init__()
        self.prefix = prefix
        self.schemas = {}
        self.writers = {}
        self.output_path = self.create_output_path(output_path, 'tfOut')

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
    def reindent(string_in, num_spaces):
        """Indents the entire string using the number of spaces specified"""
        string_in = string_in.split('\n')
        string_in = [(num_spaces * ' ') + line for line in string_in]
        string_in = '\n'.join(string_in)
        return string_in
