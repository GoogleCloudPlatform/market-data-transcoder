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

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from transcoder.message import DatacastSchema
from transcoder.output.avro.BaseAvroOutputManager import BaseAvroOutputManager


class AvroOutputManager(BaseAvroOutputManager):
    """Uses Avro library to append transcoded messages to the Avro file for the specific message type"""

    @staticmethod
    def output_type_identifier():
        return 'avro'

    def _add_schema(self, schema: DatacastSchema):
        super()._add_schema(schema)
        output_file = open(self._get_file_name(schema.name, 'avro'), 'wb')  # pylint: disable=consider-using-with
        writer = DataFileWriter(output_file, DatumWriter(), self.schemas[schema.name])
        self.writers[schema.name] = writer

    def _write_record(self, record_type_name, record):
        self.writers[record_type_name].append(record)

    def _parse_schema(self, schema_dict):
        jsoned = json.dumps(schema_dict)
        return avro.schema.parse(jsoned)
