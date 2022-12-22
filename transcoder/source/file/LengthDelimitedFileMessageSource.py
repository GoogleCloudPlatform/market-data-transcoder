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

from transcoder.source.file.FileMessageSource import FileMessageSource


class LengthDelimitedFileMessageSource(FileMessageSource):
    """Reads length delimited files and yields individual records for message consumption"""

    @staticmethod
    def source_type_identifier():
        return 'length_delimited'

    def __init__(self, file_path: str, skip_bytes: int = 0, endian: str = 'big',
                 message_skip_bytes: int = 0, message_length_byte_length: int = 2):
        super().__init__(file_path, file_open_mode='rb')
        self.skip_bytes = skip_bytes
        self.endian = endian
        self.message_skip_bytes = message_skip_bytes
        self.message_length_byte_length = message_length_byte_length

    def prepare(self):
        if self.skip_bytes > 0:
            self.file_handle.read(self.skip_bytes)

    def get_message_iterator(self):
        # pylint: disable=duplicate-code
        while True:
            if self.message_skip_bytes > 0:
                # Skip bytes based on the message_skip_bytes value
                skipped_bytes = self.file_handle.read(self.message_skip_bytes)
                if not skipped_bytes:
                    break

            # Read the message length
            msg_len_bytes = self.file_handle.read(self.message_length_byte_length)
            if not msg_len_bytes:
                break

            message_length = int.from_bytes(msg_len_bytes, self.endian)
            self.increment_count()

            # Get the message
            msg_bytes = self.file_handle.read(message_length)
            if not msg_bytes:
                break
            yield msg_bytes
            self._log_percentage_read()
