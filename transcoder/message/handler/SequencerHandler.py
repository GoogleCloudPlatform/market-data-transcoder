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

class SequencerHandler(MessageHandler):

    def __init__(self, parser: MessageParser):
        super().__init__(parser=parser)
        self.sequence_number = 0

    def append_manufactured_fields(self, schema: DatacastSchema):
        schema.fields.append(MessageHandlerIntField('sequence_number'))

    def handle(self, message: ParsedMessage):
        self.sequence_number += 1
        message.dictionary['sequence_number'] = self.sequence_number

