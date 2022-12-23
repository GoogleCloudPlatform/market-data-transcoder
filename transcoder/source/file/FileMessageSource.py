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
import logging
import os
import sys
from io import IOBase

from transcoder import LineEncoding
from transcoder.source.Source import Source, SourceFunctionNotDefinedError


class FileMessageSource(Source):
    """Abstract file message source class"""

    @staticmethod
    def source_type_identifier():
        raise SourceFunctionNotDefinedError

    def __init__(self, file_path: str, file_open_mode: str, file_encoding: str = None,
                 message_skip_bytes: int = 0, line_encoding: LineEncoding = None):
        super().__init__()
        self.path = file_path
        self.file_open_mode = file_open_mode
        self.file_encoding = file_encoding
        self.message_skip_bytes = message_skip_bytes
        self.line_encoding = line_encoding
        self.file_handle: IOBase = None
        self.file_size = 0
        self.log_percentage_read_enabled = logging.getLogger().isEnabledFor(logging.DEBUG)

    def open(self):
        if self.path is not None:
            self.file_size = os.path.getsize(self.path)
            self.file_handle = open(self.path, mode=self.file_open_mode,  # pylint: disable=consider-using-with
                                    encoding=self.file_encoding)
        elif not sys.stdin.isatty():
            if sys.stdin.seekable():
                sys.stdin.seek(0, os.SEEK_END)
                self.file_size = sys.stdin.tell()
                sys.stdin.seek(0)
            else:
                self.log_percentage_read_enabled = False
            self.file_handle = sys.stdin.buffer.raw

        self.prepare()

    def prepare(self):
        """This is called after open. Prepare file for iteration, skips etc."""

    def close(self):
        self.file_handle.close()

    def get_message_iterator(self):
        raise SourceFunctionNotDefinedError

    def _log_percentage_read(self):
        if self.file_size and self.log_percentage_read_enabled is True:
            logging.debug('Percentage read: %f%%', round((self.file_handle.tell() / self.file_size) * 100, 6))

    def decode_message(self, record):
        """Performs line decoding and message skip bytes for line encoded cases"""
        message = record
        if self.line_encoding is LineEncoding.BASE_64:
            message = base64.b64decode(record)
        elif self.line_encoding is LineEncoding.BASE_64_URL_SAFE:
            message = base64.urlsafe_b64decode(record)

        if self.message_skip_bytes > 0 and isinstance(message, bytes):
            # print(''.join('{:02x}'.format(x) for x in message))
            message = message[self.message_skip_bytes:]
            # print(''.join('{:02x}'.format(x) for x in message))

        return message
