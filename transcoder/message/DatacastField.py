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

from transcoder.output.exception import OutputFunctionNotDefinedError


class DatacastField:
    """Represents a field in a message"""

    def create_avro_field(self, part=None):
        """Abstract declaration of method for creating avro-specific field"""
        raise OutputFunctionNotDefinedError

    def create_bigquery_field(self, part=None):
        """Abstract declaration of method for creating BigQuery-specific field"""
        raise OutputFunctionNotDefinedError

    def create_json_field(self):
        """Abstract declaration of method for creating JSON-specific field"""
        raise OutputFunctionNotDefinedError

    def cast_value_to_type(self, value, field_type, is_nullable: bool = True) -> Any:  # pylint: disable=unused-argument
        """Abstract declaration of method for casting a value to its appropriate type"""
        return str(value)
