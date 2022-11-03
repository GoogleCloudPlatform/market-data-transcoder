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

from transcoder import LineEncoding
from transcoder.source import Source
from transcoder.source.file import PcapFileMessageSource, LengthDelimitedFileMessageSource, \
    LineDelimitedFileMessageSource, CmeBinaryPacketFileMessageSource


def all_source_identifiers():
    """List of all available source identifiers"""
    return [
        PcapFileMessageSource.source_type_identifier(),
        LengthDelimitedFileMessageSource.source_type_identifier(),
        LineDelimitedFileMessageSource.source_type_identifier(),
        CmeBinaryPacketFileMessageSource.source_type_identifier()
    ]


def get_message_source(source_name: str,  # pylint: disable=too-many-arguments
                       source_file_encoding: str, source_file_format_type: str,
                       endian: str, skip_bytes: int = 0, skip_lines: int = 0,
                       message_skip_bytes: int = 0, message_length_byte_length: int = 2,
                       line_encoding: LineEncoding = None) -> Source:
    """Returns a Source implementation instance based on the supplied source name"""
    source: Source = None
    if source_file_format_type == PcapFileMessageSource.source_type_identifier():
        source = PcapFileMessageSource(source_name, message_skip_bytes=message_skip_bytes)
    elif source_file_format_type == LengthDelimitedFileMessageSource.source_type_identifier():
        source = LengthDelimitedFileMessageSource(source_name, skip_bytes=skip_bytes,
                                                  message_skip_bytes=message_skip_bytes,
                                                  message_length_byte_length=message_length_byte_length)
    elif source_file_format_type == LineDelimitedFileMessageSource.source_type_identifier():
        source = LineDelimitedFileMessageSource(source_name, encoding=source_file_encoding,
                                                skip_lines=skip_lines,
                                                line_encoding=line_encoding, message_skip_bytes=message_skip_bytes)
    elif source_file_format_type == CmeBinaryPacketFileMessageSource.source_type_identifier():
        source = CmeBinaryPacketFileMessageSource(source_name, endian, skip_bytes=skip_bytes,
                                                  message_skip_bytes=message_skip_bytes,
                                                  message_length_byte_length=message_length_byte_length)
    else:
        raise UnsupportedFileTypeError(f'Source {source_name} is not supported')
    return source


class UnsupportedFileTypeError(Exception):
    """Exception that is raised when a file source type name cannot resolve to a child Source class"""
