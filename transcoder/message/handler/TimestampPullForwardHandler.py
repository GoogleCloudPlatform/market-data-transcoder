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

from transcoder.message import MessageParser, ParsedMessage, DatacastSchema
from transcoder.message.handler.MessageHandler import MessageHandler
from transcoder.message.handler.MessageHandlerIntField import MessageHandlerIntField


class TimestampPullForwardHandler(MessageHandler):
    """Custom message handler that stores the 'second' value from the last message of type 'time_message',
    and carries it forward into other message types not of type 'time_message'"""

    def __init__(self, parser: MessageParser):
        super().__init__(parser=parser)
        self.last_timestamp_message = None
        self.last_epoch_seconds = None
        self.time_message_type_name = 'time_message'
        self.time_value_field_name = 'second'
        self.new_timestamp_field_name = 'timestamp_seconds'

    def append_manufactured_fields(self, schema: DatacastSchema):
        if schema.name != self.time_message_type_name:
            schema.fields.append(MessageHandlerIntField(self.new_timestamp_field_name))

    def handle(self, message: ParsedMessage):
        if message.name == self.time_message_type_name:
            self.last_timestamp_message = message.dictionary
            self.last_epoch_seconds = int(message.dictionary[self.time_value_field_name])
        else:
            message.dictionary[self.new_timestamp_field_name] = self.last_epoch_seconds
