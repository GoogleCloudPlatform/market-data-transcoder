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

from transcoder.message import DatacastSchema, ParsedMessage
from transcoder.message.exception import ParserFunctionNotDefinedError


class DatacastParser:
    """Class encapsulating message parsing and processing functionality """

    @staticmethod
    def supported_factory_types():
        """Static method for retrieving list of provider-specific factory classes"""
        raise ParserFunctionNotDefinedError

    def __init__(self, sampling_count: int = None, stats_only: bool = False,
                 message_type_inclusions: str = None, message_type_exclusions: str = None):
        self.sampling_count = sampling_count
        self.stats_only = stats_only
        self.message_type_inclusions = message_type_inclusions.split(
            ',') if message_type_inclusions is not None else None
        self.message_type_exclusions = message_type_exclusions.split(
            ',') if message_type_exclusions is not None else None
        self.use_message_type_filtering = message_type_inclusions is not None or message_type_exclusions is not None
        self.use_sampling = sampling_count is not None and sampling_count > 0
        self.record_count = 0
        self.summary_count = {}
        self.total_schema_count = 0
        self.error_summary_count = {}

    @property
    def record_type_count(self):
        """Method returning count of record types in a given source"""
        return self.summary_count

    @property
    def total_record_count(self):
        """Method returning count of all records in a given source"""
        return self.record_count

    @property
    def error_record_type_count(self):
        """Method returning count of all errored records by type"""
        return self.error_summary_count

    def process_schema(self) -> [DatacastSchema]:
        """Gets message names from schema file, filters messages to include, sets count dict"""
        schema_list = self._process_schema()
        filtered_list = list(filter(lambda x: self.__include_message_type(x.name), schema_list))
        self.total_schema_count = len(filtered_list)
        for name in list(map(lambda x: x.name, filtered_list)):
            self.summary_count[name] = 0
        return filtered_list

    def _process_schema(self) -> [DatacastSchema]:
        raise ParserFunctionNotDefinedError

    def process_message(self, raw_msg) -> ParsedMessage:
        """Wraps _process_message with count and inclusion behavior"""
        message = self._process_message(raw_msg)

        if message is None:
            return None

        if self.use_sampling is True and self.get_summary_count(message.name) >= self.sampling_count:
            message.ignored = True
            return message

        if self.__include_message_type(message.name) is False:
            message.ignored = True
            return message

        self.increment_summary_count(message.name)

        if self.stats_only is True:
            message.ignored = True
            return message

        message = self._parse_message(message)
        return message

    def __include_message_type(self, msg_type):
        if self.use_message_type_filtering is True:
            msg_type_str = str(msg_type)
            if self.message_type_inclusions is not None and msg_type_str not in self.message_type_inclusions:
                return False
            if self.message_type_exclusions is not None and msg_type_str in self.message_type_exclusions:
                return False
        return True

    def _process_message(self, raw_msg) -> ParsedMessage:
        raise ParserFunctionNotDefinedError

    def _parse_message(self, message: ParsedMessage) -> ParsedMessage:
        raise ParserFunctionNotDefinedError

    def _process_fields(self, message: ParsedMessage):
        raise ParserFunctionNotDefinedError

    def increment_summary_count(self, message_name: str):
        """Increments message count by type"""
        self.record_count += 1
        if message_name not in self.summary_count:
            self.summary_count[message_name] = 0
        self.summary_count[message_name] += 1

    def increment_error_summary_count(self, message_name: str = 'UNKNOWN'):
        """Increments error count by message type"""
        if message_name not in self.error_summary_count:
            self.error_summary_count[message_name] = 0
        self.error_summary_count[message_name] += 1

    def get_summary_count(self, message_name: str):
        """Returns summary count by message type"""
        return self.summary_count.get(message_name, 0)
