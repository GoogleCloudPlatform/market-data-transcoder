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

from transcoder.output.google_cloud.terraform.GcpTerraformOutputManager import GCPTerraformOutputManager


class PubSubTerraformOutputManager(GCPTerraformOutputManager):
    """Output manager for representing schemas as PubSub topic and schema terraform resources"""

    @staticmethod
    def output_type_identifier():
        return 'pubsub_terraform'

    def __init__(self, project_id: str, output_encoding: str, output_prefix: str = None,
                 create_schema_enforcing_topics: bool = True):
        super().__init__()

    def write_record(self, record_type_name, record):
        pass
