#
# MIT License
#
# Copyright (c) 2018 TradeForecaster Global Markets, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#


from third_party.sbedecoder.message import TypeMessageField
from transcoder.message import DatacastField
from transcoder.message.DatacastGroup import DatacastGroup
from transcoder.message.DatacastParser import DatacastParser
from transcoder.message.DatacastSchema import DatacastSchema
from transcoder.message.ParsedMessage import ParsedMessage


class SBEParser(DatacastParser):
    """SBE message parser"""

    @staticmethod
    def supported_factory_types():
        return ['asx', 'cme', 'memx']

    def __init__(self, msg_factory, sampling_count: int = None, message_type_inclusions: str = None,
                 message_type_exclusions: str = None, stats_only: bool = False):
        super().__init__(sampling_count=sampling_count, message_type_inclusions=message_type_inclusions,
                         message_type_exclusions=message_type_exclusions, stats_only=stats_only)
        self.factory = msg_factory

    def parse(self, message_buffer, offset=0):
        """Passes a message buffer to the factory for processing"""
        msg_offset = offset
        while msg_offset < len(message_buffer):
            message, message_size = self.factory.build(message_buffer, msg_offset)
            msg_offset += message_size
            yield message

    def _process_schema(self):
        schemas: [DatacastSchema] = []
        for msg in self.factory.schema.messages:
            message_name = msg['name']
            message_id = int(msg['id'])
            message_schema = self.factory.schema.message_map[message_id]
            fields: [DatacastField] = self.traverse_schema(message_name, message_schema)
            schemas.append(DatacastSchema(message_id, message_name, fields))
        return schemas

    def traverse_schema(self, message_name, message_schema):
        """Traverses message schema and builds the schema to a Datacast representation"""
        fields: [DatacastField] = []
        for field in message_schema.fields:
            if field.id is not None:
                fields.append(field)

        # Iterate groups which is an aray of SBERepeatingGroupContainer
        for group in message_schema.groups:
            # TODO: https://github.com/GoogleCloudPlatform/market-data-transcoder/issues/35 - Handle nested groups
            nested_groups = group.groups  # pylint: disable=unused-variable

            _group = DatacastGroup(group.name)
            _group_fields = self.traverse_schema(message_name, group)
            _group.fields.extend(_group_fields)
            fields.append(_group)

        return fields

    def _process_message(self, raw_msg) -> ParsedMessage:
        # TODO: Problematic - loops through will only ever return once while potentially parsing multiple messages?
        for sbe_msg in self.parse(raw_msg, 0):
            return ParsedMessage(sbe_msg.message_id, sbe_msg.name, raw_message=sbe_msg)

    def _parse_message(self, message: ParsedMessage) -> ParsedMessage:
        sbe_msg = message.raw_message
        try:
            message.dictionary = self.process_field(sbe_msg.fields, sbe_msg.groups)
        except Exception as ex:  # pylint: disable=broad-except
            message.exception = ex
        return message

    def process_field(self, fields, groups):
        """Processes the SBE message fields and puts the values into a readable dictionary"""
        output_result = {}
        field_exclusions = []
        for field in fields:
            if field.name in field_exclusions:
                continue

            if field.id is not None:
                if isinstance(field, TypeMessageField) and field.is_string_type is True and field.value is not None:
                    output_result[field.name] = field.value.strip()
                else:
                    output_result[field.name] = field.value

        # Iterate array of SBERepeatingGroupContainer
        for group in groups:
            group_name = group.name

            # Create new group
            if group_name not in output_result:
                output_result[group_name] = []

            for repeating_group in group:
                output_result[group_name].append(self.process_field(repeating_group.fields, repeating_group.groups))

        return output_result
