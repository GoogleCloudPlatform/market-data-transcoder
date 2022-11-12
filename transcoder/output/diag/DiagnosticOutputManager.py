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

import yaml

from transcoder.message import DatacastField, DatacastSchema
from transcoder.output import OutputManager
from transcoder.output.exception import OutputFunctionNotDefinedError, OutputManagerSchemaError


class DiagnosticOutputManager(OutputManager):
    """Output manager for representing  messages in diagnostic notation"""

    def __init__(self, schema_max_workers=5, lazy_create_resources: bool = False):
        self.lazy_create_resources = True
        self.schema_definitions = {}

    def write_record(self, record_type_name, record):
        print(yaml.dump(record))

    def wait_for_schema_creation(self):
        pass
