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

import math

from transcoder.message import MessageParser, ParsedMessage, DatacastSchema
from transcoder.message.handler.MessageHandler import MessageHandler
from transcoder.message.handler.MessageHandlerFloatField import MessageHandlerFloatField


class CmeBinaryPacketHandler(MessageHandler):
    """CME binary package message handler which appends and computes a new field md_entry_calculated_px"""

    def __init__(self, parser: MessageParser):
        super().__init__(parser=parser)

    def append_manufactured_fields(self, schema: DatacastSchema):
        if schema.name != 'MDIncrementalRefreshOrderBook47':
            schema.fields.append(MessageHandlerFloatField('timestamp_seconds'))

    def handle(self, message: ParsedMessage):
        if message.name == 'MDIncrementalRefreshOrderBook47':  # pylint: disable=too-many-nested-blocks
            md_entries = message.dictionary.get('no_md_entries', None)
            if md_entries is not None:
                if len(md_entries) > 0:
                    for md_entry in md_entries:
                        md_entry_px = md_entry.get('md_entry_px', None)
                        if md_entry_px is not None:
                            mantissa = md_entry_px.get('mantissa', None)
                            exponent = md_entry_px.get('exponent', None)
                            if mantissa is not None and exponent is not None:
                                md_entry['md_entry_calculated_px'] = float(mantissa) * math.pow(10, exponent)
