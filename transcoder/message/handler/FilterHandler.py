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

from transcoder.message import MessageParser, ParsedMessage
from transcoder.message.handler.MessageHandler import MessageHandler

class FilterHandler(MessageHandler):
    """ Handler for filtering messages by a single field's value """

    def __init__(self, parser: MessageParser, config=None):
        super().__init__(parser=parser, config=config)

    def handle(self, message: ParsedMessage):
        if self.config.keys is not None and len(self.config.keys()) > 0:
            if not message.dictionary[list(self.config.keys())[0]] == list(self.config.values())[0]:
                message.dictionary = None
