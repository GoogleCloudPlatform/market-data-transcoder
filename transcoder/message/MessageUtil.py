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

from third_party.pyfixmsg.parser import FixParser
from third_party.sbedecoder import SBEParser
from transcoder.message import DatacastParser
from transcoder.message.exception import MessageParserNotDefinedError
from transcoder.message.factory.MessageFactory import get_message_factory


def get_message_parser(factory: str, schema_file_path: str,
                       # pylint: disable=too-many-arguments,too-many-positional-arguments
                       stats_only: bool = False,
                       message_type_inclusions: str = None, message_type_exclusions: str = None,
                       fix_header_tags: str = None, fix_separator: int = 1) -> DatacastParser:
    """Returns a DatacastParser instance based on the supplied factory name"""
    message_parser: DatacastParser = None
    if factory in SBEParser.supported_factory_types():
        message_factory = get_message_factory(factory, schema_file_path)
        message_parser = SBEParser(message_factory,
                                   message_type_inclusions=message_type_inclusions,
                                   message_type_exclusions=message_type_exclusions,
                                   stats_only=stats_only)
    elif factory in FixParser.supported_factory_types():
        message_parser = FixParser(schema_file_path=schema_file_path,
                                   message_type_inclusions=message_type_inclusions,
                                   message_type_exclusions=message_type_exclusions,
                                   fix_header_tags=fix_header_tags, fix_separator=fix_separator,
                                   stats_only=stats_only)
    else:
        raise MessageParserNotDefinedError

    return message_parser


def parse_handler_config(handler_config_string: str) -> dict:
    """
    Extracts the configuration parameters attached to the CLI handler option,
    in the format:

    FirstHandler:<param>=<value>,SecondHandler:<param>=<value>

    For example:

    --message_handlers SequencerHandler,FilterHandler:field=value

    would run a SequencerHandler without a config, then pass a single-element dict of field: value to FilterHandler via the config object.

    This routine manufactures the configuration for the Handler from the CLI string

    """
    if handler_config_string.find(':') != -1:
        config = {}
        params = handler_config_string.split(':')[1]
        keyval = params.split('=')
        config[keyval[0]] = keyval[1]
        return config
    return None
