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

from transcoder.source.file import LengthDelimitedFileMessageSource


class CmeBinaryPacketFileMessageSource(LengthDelimitedFileMessageSource):
    """CME binary package file message source implementation. Derives from length delimited source and overrides the
    message slicing logic """

    @staticmethod
    def source_type_identifier():
        return 'cme_binary_packet'

    def __init__(self, file_path: str, endian: str, skip_bytes: int = 0,
                 message_skip_bytes: int = 0, message_length_byte_length: int = 2):
        super().__init__(file_path, skip_bytes=skip_bytes, endian=endian,
                         message_skip_bytes=message_skip_bytes,
                         message_length_byte_length=message_length_byte_length)

    def get_message_iterator(self):
        while self.file_handle.tell() < self.file_size:
            if self.message_skip_bytes > 0:
                # Skip the channel id 2 bytes
                self.file_handle.read(self.message_skip_bytes)

            # read the parent message length 2 bytes
            message_length = int.from_bytes(self.file_handle.read(self.message_length_byte_length), self.endian)
            remaining_message_length = message_length

            # skip binary packet header 12 bytes
            self.file_handle.seek(12, 1)

            # read message header message size 2 bytes
            child_message_length = int.from_bytes(self.file_handle.read(self.message_length_byte_length), self.endian)

            remainder = child_message_length - self.message_length_byte_length
            self.increment_count()

            result = self.file_handle.read(remainder)
            # print(''.join('{:02x}'.format(x) for x in result))
            yield result

            # Subtract out the binary packet header 12 bytes
            remaining_message_length = remaining_message_length - child_message_length - 12

            while remaining_message_length > 0:
                child_message_length = int.from_bytes(self.file_handle.read(self.message_length_byte_length),
                                                      self.endian)
                remainder = child_message_length - self.message_length_byte_length
                self.increment_count()
                yield self.file_handle.read(remainder)
                remaining_message_length = remaining_message_length - child_message_length

            self._log_percentage_read()
