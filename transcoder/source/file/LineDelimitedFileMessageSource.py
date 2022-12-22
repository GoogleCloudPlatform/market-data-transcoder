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

import sys

from transcoder import LineEncoding
from transcoder.source.file import FileMessageSource


class LineDelimitedFileMessageSource(FileMessageSource):
    """Reads line delimited files and yields individual records for message consumption"""

    @staticmethod
    def source_type_identifier():
        return 'line_delimited'

    def __init__(self, file_path: str, encoding: str, skip_lines: int = 0,
                 message_skip_bytes: int = 0, line_encoding: LineEncoding = None):
        super().__init__(file_path, file_open_mode='rt', file_encoding=encoding,
                         line_encoding=line_encoding, message_skip_bytes=message_skip_bytes)
        self.skip_lines = skip_lines

    def prepare(self):
        # file_size is only determinable on seekable input
        if sys.stdin.seekable() is True and self.file_size == 0:
            return
        if self.skip_lines > 0:
            for _ in range(self.skip_lines):
                self.file_handle.readline()

    def get_message_iterator(self):
        while line := self.file_handle.readline():
            self.increment_count()
            yield self.decode_message(line)
            self._log_percentage_read()
