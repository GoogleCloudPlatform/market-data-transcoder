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

from transcoder.message import ParsedMessage, DatacastSchema
from transcoder.message.handler.MessageHandler import MessageHandler
from transcoder.message.handler.MessageHandlerIntField import MessageHandlerIntField


class SequencerHandler(MessageHandler):
    """ Message handler to append a sequencer number to all messages transcoded from an arbitrary source.
    Particularly useful when transcoding messages encapsulated in POSIX files where the original sequence numbers were found within the pocket header and not the message itself """

    def __init__(self, config=None):
        super().__init__(config=config)
        if config is not None:
            if 'field_name' in config:
                self.sequence_number_field_name = config['field_name']
            else:
                self.sequence_number_field_name = 'sequence_number'
        else:
            self.sequence_number_field_name = 'sequence_number'
        self.sequence_number = 0

    def append_manufactured_fields(self, schema: DatacastSchema):
        schema.fields.append(MessageHandlerIntField(self.sequence_number_field_name))

    def handle(self, message: ParsedMessage):
        if message.ignored is False:
            self.sequence_number += 1
            message.dictionary[self.sequence_number_field_name] = self.sequence_number
