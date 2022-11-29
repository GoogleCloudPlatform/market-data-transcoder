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

import logging

import dpkt

from transcoder.source.file.FileMessageSource import FileMessageSource


class PcapFileMessageSource(FileMessageSource):
    """Reads pcap files and yields individual records for message consumption"""

    @staticmethod
    def source_type_identifier():
        return 'pcap'

    def __init__(self, file_path: str, message_skip_bytes: int = 0, length_threshold: int = 50):
        super().__init__(file_path, file_open_mode='rb')
        self.message_skip_bytes = message_skip_bytes
        self.pcap_reader: dpkt.pcap.Reader = None
        self.length_threshold = length_threshold

    def prepare(self):
        self.pcap_reader = dpkt.pcap.Reader(self.file_handle)

    def get_message_iterator(self):
        # pylint: disable=unused-variable
        # pylint: disable=no-member
        for timestamp, packet in self.pcap_reader:
            ethernet = dpkt.ethernet.Ethernet(packet)
            if not isinstance(ethernet.data, dpkt.ip.IP):
                logging.debug('Packet type not supported %s\n', ethernet.data.__class__.__name__)
            else:
                proto = ethernet.ip.tcp if 'tcp' in ethernet.ip.__dict__.keys() else ethernet.ip.udp
                pck_len = len(proto.data)
                if pck_len > self.length_threshold:
                    stripped = proto.data[self.message_skip_bytes:pck_len]
                    yield stripped
                    self.increment_count()
            self._log_percentage_read()
