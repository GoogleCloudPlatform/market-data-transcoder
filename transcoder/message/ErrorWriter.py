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

import base64
import os
import sys
import time
from enum import Enum

from transcoder.message import ParsedMessage


class TranscodeStep(Enum):
    """Enum of steps at which an error may be encountered"""
    UNKNOWN = 'UNKNOWN'
    DECODE_MESSAGE = 'decode_message'
    PARSE_MESSAGE = 'parse_message'
    EXECUTE_HANDLERS = 'execute_handlers'
    EXECUTE_HANDLER = 'execute_handler'
    WRITE_OUTPUT_RECORD = 'write_output_record'

    @classmethod
    def _missing_(cls, value):
        return TranscodeStep.UNKNOWN

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class ErrorWriter:
    """Persist data about errors to file"""

    def __init__(self, prefix: str, output_path: str = None):
        self.prefix: str = prefix
        self.step: TranscodeStep = TranscodeStep.UNKNOWN
        self.note: str = ''

        if output_path is None:
            rel_path = "errorOut"
            main_script_dir = os.path.dirname(sys.argv[0])
            self.output_path = os.path.join(main_script_dir, rel_path)
        else:
            self.output_path = output_path

        self.file = None

    def __get_file_name(self, name, extension):
        epoch_time = str(time.time())
        return self.output_path + '/' + name + '-' + epoch_time + '.' + extension

    def set_step(self, step: TranscodeStep, note: str = ''):
        """Sets step during which error is encountered"""
        self.step = step
        self.note = note

    def write_error(self, raw_record, message: ParsedMessage, exception: Exception):
        """Write data about error to file"""
        if self.file is None:
            exists = os.path.exists(self.output_path)
            if not exists:
                os.makedirs(self.output_path)

            # Only create error file if errors exist
            self.file = open(self.__get_file_name(self.prefix, 'out'),  # pylint: disable=consider-using-with
                             mode='w', encoding='utf-8')
            self.file.write('time, message_type, message_name, failed_step, exception, data\n')

        encoded = self.__encode_source_message(raw_record)
        ex_str = str(exception).replace('\r', '').replace('\n', '').replace(',', ' ')
        epoch_time = time.time()
        current_step = f'{self.step}-{self.note}'
        if message is not None:
            out_str = f'{epoch_time}, {message.type}, {message.name}, {current_step}, {ex_str}, '
        else:
            out_str = f'{epoch_time},,, {self.step}, {ex_str}, '
        self.file.write(out_str + encoded + '\n')

    @staticmethod
    def __encode_source_message(record):
        if record is None:
            return ''
        if isinstance(record, bytes):
            return base64.b64encode(record).decode('utf-8')
        if isinstance(record, str):
            return base64.b64encode(record.encode('utf-8')).decode('utf-8')
        return base64.b64encode(record)

    def __del__(self):
        if self.file is not None:
            self.file.close()
