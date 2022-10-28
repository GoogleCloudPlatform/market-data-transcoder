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

from third_party.pyfixmsg.parser import FixParser
from third_party.sbedecoder import SBEParser
from transcoder.message import DatacastParser
from transcoder.message.exception import MessageParserNotDefinedError
from transcoder.message.factory.MessageFactory import get_message_factory


def get_message_parser(factory: str, schema_file_path: str,
                       sampling_count: int = None, stats_only: bool = False,
                       message_type_inclusions: str = None, message_type_exclusions: str = None,
                       fix_header_tags: str = None, fix_separator: int = 1) -> DatacastParser:
    """Returns a DatacastParser instance based on the supplied factory name"""
    message_parser: DatacastParser = None
    if factory in SBEParser.supported_factory_types():
        message_factory = get_message_factory(factory, schema_file_path)
        message_parser = SBEParser(message_factory, sampling_count=sampling_count,
                                   message_type_inclusions=message_type_inclusions,
                                   message_type_exclusions=message_type_exclusions,
                                   stats_only=stats_only)
    elif factory in FixParser.supported_factory_types():
        message_parser = FixParser(schema_file_path=schema_file_path, sampling_count=sampling_count,
                                   message_type_inclusions=message_type_inclusions,
                                   message_type_exclusions=message_type_exclusions,
                                   fix_header_tags=fix_header_tags, fix_separator=fix_separator,
                                   stats_only=stats_only)
    else:
        raise MessageParserNotDefinedError

    return message_parser
