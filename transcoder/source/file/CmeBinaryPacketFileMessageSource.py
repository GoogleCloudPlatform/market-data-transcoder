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
        # pylint: disable=duplicate-code
        while True:
            if self.message_skip_bytes > 0:
                # Skip the channel id 2 bytes
                skipped_bytes = self.file_handle.read(self.message_skip_bytes)
                if not skipped_bytes:
                    break

            # Read the parent message length 2 bytes
            parent_msg_bytes = self.file_handle.read(self.message_length_byte_length)
            if not parent_msg_bytes:
                break
            message_length = int.from_bytes(parent_msg_bytes, self.endian)
            remaining_message_length = message_length

            # Skip binary packet header 12 bytes
            # Unable seek on a stream,
            # self.file_handle.seek(12, 1)
            packet_header_bytes = self.file_handle.read(12)
            if not packet_header_bytes:
                break

            # Read message header message size 2 bytes
            msg_len_bytes = self.file_handle.read(self.message_length_byte_length)
            if not msg_len_bytes:
                break
            child_message_length = int.from_bytes(msg_len_bytes, self.endian)

            remainder = child_message_length - self.message_length_byte_length
            self.increment_count()

            first_msg_bytes = self.file_handle.read(remainder)
            if not first_msg_bytes:
                break
            # print(''.join('{:02x}'.format(x) for x in result))
            yield first_msg_bytes

            # Subtract out the binary packet header 12 bytes
            remaining_message_length = remaining_message_length - child_message_length - 12

            while remaining_message_length > 0:
                child_msg_len_bytes = self.file_handle.read(self.message_length_byte_length)
                if not child_msg_len_bytes:
                    break
                child_message_length = int.from_bytes(child_msg_len_bytes, self.endian)
                remainder = child_message_length - self.message_length_byte_length
                self.increment_count()
                child_msg_bytes = self.file_handle.read(remainder)
                if not child_msg_bytes:
                    break
                yield child_msg_bytes
                remaining_message_length = remaining_message_length - child_message_length

            self._log_percentage_read()
