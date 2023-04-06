#
# Copyright 2023 Google LLC
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

import struct
from transcoder.output import OutputManager


class LengthDelimitedOutputManager(OutputManager):
    """Output manager for representing  messages in diagnostic notation"""

    @staticmethod
    def output_type_identifier():
        return 'length_delimited'

    def write_record(self, record_type_name, record):
        byte_len = struct.pack('>L', len(record))
        sys.stdout.buffer.write(byte_len + record)

        
