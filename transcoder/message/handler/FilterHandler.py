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

from transcoder.message import ParsedMessage
from transcoder.message.handler.MessageHandler import MessageHandler


class FilterHandler(MessageHandler):
    """ Handler for filtering messages by a single field's value """

    def handle(self, message: ParsedMessage):
        if self.config.keys is not None and len(self.config.keys()) > 0:
            prop = list(self.config.keys())[0]
            val = list(self.config.values())[0]
            if prop in message.dictionary:
                if not self.match(message.dictionary[prop], val):
                    message.ignored = True
            else:
                message.ignored = True

    @staticmethod
    def match(message_value, filter_value):
        """ Compares the filter criteria based on the message property type """
        field_value_type = type(message_value)
        if field_value_type is None:
            return message_value is None
        if field_value_type == str:
            return message_value == filter_value  # already a str
        if field_value_type == int:
            return message_value == int(
                filter_value) if filter_value != '' and filter_value is not None else message_value == '' or message_value is None
        if field_value_type == float:
            return message_value == float(
                filter_value) if filter_value != '' and filter_value is not None else message_value == '' or message_value is None

        # TODO: will need throw exception instead
        return False
