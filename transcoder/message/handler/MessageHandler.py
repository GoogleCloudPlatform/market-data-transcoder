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

from transcoder.message import ParsedMessage


class MessageHandler:
    """Base class for handlers of specific message types"""

    def __init__(self, config=None):
        self.config = config
        self.all_value: str = '__ALL__'

    @property
    def supports_all_message_types(self):
        """Returns whether handler class supports all message types within a given source"""
        return True

    @property
    def supported_message_types(self):
        """Returns handler's supported message types"""
        return []

    def append_manufactured_fields(self, schema):  # pylint: disable=unused-argument
        """Extend for handler-specific logic for appending manufactured field to message"""
        return None

    def handle(self, message: ParsedMessage):
        """Extend for handler-specific logic for message processing"""
        raise Exception  # pylint: disable=broad-exception-raised
