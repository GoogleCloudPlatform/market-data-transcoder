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

# pylint: disable=invalid-name

import logging

from third_party.pyfixmsg import RepeatingGroup
from third_party.pyfixmsg.codecs.stringfix import Codec
from third_party.pyfixmsg.exception.FixSchemaNotDefinedError import FixSchemaNotDefinedError
from third_party.pyfixmsg.fixmessage import FixFragment, FixMessage
from third_party.pyfixmsg.reference import FixSpec, FixTag, Component, Group
from transcoder.message.DatacastField import DatacastField
from transcoder.message.DatacastGroup import DatacastGroup
from transcoder.message.DatacastParser import DatacastParser
from transcoder.message.DatacastSchema import DatacastSchema
from transcoder.message.ParsedMessage import ParsedMessage


class FixParser(DatacastParser):
    """Fix message parser"""

    @staticmethod
    def supported_factory_types():
        return ['fix']

    def __init__(self, schema_file_path: str, sampling_count: int = None, message_type_inclusions: str = None,
                 message_type_exclusions: str = None, fix_header_tags: str = None, fix_separator: int = 1,
                 stats_only: bool = False):
        super().__init__(sampling_count=sampling_count, message_type_inclusions=message_type_inclusions,
                         message_type_exclusions=message_type_exclusions, stats_only=stats_only)
        self.schema_file_path = schema_file_path
        self.fix_header_tags = fix_header_tags
        self.fix_separator = fix_separator
        self.spec = FixSpec(schema_file_path)

        # The codec will use the given spec to find repeating groups
        # The codec will produce FixFragment objects inside repeating groups
        self.codec = Codec(spec=self.spec, fragment_class=FixFragment)

        self.header_tags = []

    def _process_schema(self):
        _header_tags = []
        if self.fix_header_tags is not None:
            for tag in self.fix_header_tags.split(','):
                _header_tags.append(self.spec.tags.by_tag(int(tag)))

        schemas: [DatacastSchema] = []
        for _, value in self.spec.msg_types.items():
            message_id = value.msgtype
            message_name = value.name
            field_composition = value.composition
            fields: [DatacastField] = self.traverse_schema(message_name, field_composition)
            fields = _header_tags + self.spec.header_tags + fields
            schemas.append(DatacastSchema(message_id, message_name, fields))
        return schemas

    def traverse_schema(self, message_name, composition):
        """Traverses message composition and builds the schema to a Datacast representation"""
        fields: [DatacastField] = []
        for element, _ in composition:
            if isinstance(element, FixTag) and element.tag:
                self.unique_append(message_name, fields, [element])
            elif isinstance(element, Component):
                elements = self.traverse_schema(message_name, element.composition)
                self.unique_append(message_name, fields, elements)
            elif isinstance(element, Group):
                _group = DatacastGroup(element.name)
                _group_fields = self.traverse_schema(message_name, element.composition)
                _group.fields.extend(_group_fields)
                fields.append(_group)
            else:
                raise Exception('Composition field type not handled')
        return fields

    @staticmethod
    def unique_append(message_name, fields, elements):
        """Uniquely adds message fields, if a field was already included it will be ignored"""
        for element in elements:
            is_duplicate = False
            for field in fields:
                if not isinstance(field, DatacastGroup) and field.is_equal(element):
                    is_duplicate = True
                    break
            if is_duplicate is False:
                fields.append(element)
            else:
                logging.warning('Duplicate field found for message type %s: %s', message_name, element)

    def _process_message(self, raw_msg) -> ParsedMessage:
        fix_msg = FixMessage()
        fix_msg.codec = self.codec
        separator = chr(self.fix_separator)
        msg = fix_msg.load_fix(raw_msg, separator=separator)
        msg_type_id = msg[35]
        message_type = self.spec.msg_types.get(msg_type_id, None)

        if message_type is None:
            raise FixSchemaNotDefinedError(f'Schema not found for msgtype: {msg_type_id}')

        message_name = message_type.name
        return ParsedMessage(msg_type_id, message_name, raw_message=msg)

    def _parse_message(self, message: ParsedMessage) -> ParsedMessage:
        try:
            fix_msg = message.raw_message
            message.dictionary = self.process_field(fix_msg, fix_msg.items())
        except Exception as ex:  # pylint: disable=broad-except
            message.exception = ex
        return message

    def process_field(self, msg, items):
        """Processes the Fix message fields and puts the values into a readable dictionary"""
        if not isinstance(items, list):  # pylint: disable=no-else-return
            dictionary = {}
            for key, value in items:
                if key in self.header_tags:
                    continue
                tag = msg.codec.spec.tags.by_tag(key)
                if isinstance(value, RepeatingGroup):
                    dictionary[tag.name] = self.process_field(msg, value)
                else:
                    dictionary[tag.name] = tag.cast_value_to_type(value, tag.type, False)
            return dictionary
        else:
            array = []
            # Repeating Groups array
            for item in items:
                record = {}
                for key, value in item.items():
                    tag = msg.codec.spec.tags.by_tag(key)
                    if isinstance(value, RepeatingGroup):
                        array.append(self.process_field(msg, value))
                    else:
                        record[tag.name] = tag.cast_value_to_type(value, tag.type, False)
                array.append(record)

            return array
