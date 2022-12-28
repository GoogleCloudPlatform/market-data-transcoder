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

# pylint: skip-file

import logging
from struct import unpack_from

import numpy as np
from google.cloud import bigquery

from third_party.sbedecoder.typemap import TypeMap
from transcoder.message.DatacastField import DatacastField

null_value = {
    'int8': np.iinfo(np.int8).max,
    'uint8': np.iinfo(np.uint8).max,
    'int16': np.iinfo(np.int16).max,
    'uint16': np.iinfo(np.uint16).max,
    'int32': np.iinfo(np.int32).max,
    'uint32': np.iinfo(np.uint32).max,
    'int64': np.iinfo(np.int64).max,
    'uint64': np.iinfo(np.uint64).max
}

# https://json-schema.org/understanding-json-schema/reference/type.html
json_type_map = {
    'int8': 'integer',
    'uint8': 'integer',
    'int16': 'integer',
    'uint16': 'integer',
    'int32': 'integer',
    'uint32': 'integer',
    'int64': 'integer',
    'uint64': 'integer',
    'char': 'string',
    'double': 'number',
    'float': 'number'
}

# https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions
avro_type_map = {
    'int8': 'int',  # BQ converts to INTEGER
    'uint8': 'int',  # BQ converts to INTEGER
    'int16': 'int',  # BQ converts to INTEGER
    'uint16': 'int',  # BQ converts to INTEGER
    'int32': 'long',  # BQ converts to INTEGER
    'uint32': 'long',  # BQ converts to INTEGER
    'int64': 'long',  # BQ converts to INTEGER
    'uint64': 'double',  # BQ converts to FLOAT
    'char': 'string',  # BQ converts to STRING
    'double': 'double',  # BQ converts to FLOAT
    'float': 'float'  # BQ converts to FLOAT
}

# https://cloud.google.com/pubsub/docs/bigquery#schema_compatibility
bigquery_type_map = {
    'int8': 'INTEGER',
    'uint8': 'INTEGER',
    'int16': 'INTEGER',
    'uint16': 'INTEGER',
    'int32': 'INTEGER',
    'uint32': 'INTEGER',
    'int64': 'INTEGER',
    'uint64': 'FLOAT',
    'char': 'STRING',
    'double': 'FLOAT',
    'float': 'FLOAT'
}


def is_empty_byte_array(_raw_value: bytes) -> bool:
    zero_byte_arr = False
    if isinstance(_raw_value, bytes) is True:
        zero_byte_arr = True
        for byte in _raw_value:
            if byte != 0:
                zero_byte_arr = False
                break
    return zero_byte_arr


def get_bool_value(_raw_value) -> bool:
    if _raw_value in (1, 'True'):
        return True
    return False


class SBEMessageField(DatacastField):
    def __init__(self):
        self.name = None
        self.original_name = None
        self.id = None
        self.description = None
        self.msg_buffer = None
        self.msg_offset = None
        self.unpack_fmt = None
        self.field_offset = 0
        self.relative_offset = 0
        self.semantic_type = None

    def wrap(self, msg_buffer, msg_offset, relative_offset=0):
        self.msg_buffer = msg_buffer
        self.msg_offset = msg_offset
        self.relative_offset = relative_offset

    @property
    def value(self):
        return None

    @property
    def raw_value(self):
        return None

    @property
    def is_bool_type(self):
        return self.semantic_type == 'bool'

    @property
    def is_int_type(self):
        return False

    def __str__(self, raw=False):
        if raw and self.value != self.raw_value:
            return f'{self.name}: {str(self.value)} ({str(self.raw_value)}'
        return f'{self.name}: {str(self.value)}'

    @staticmethod
    def get_json_field_type(part: DatacastField = None):
        field = part
        if isinstance(field, TypeMessageField):
            if field.is_bool_type is True:
                return 'boolean'
            else:
                mapped_type = json_type_map[field.primitive_type]
                return mapped_type
        elif isinstance(field, EnumMessageField):
            if field.is_bool_type is True:
                return 'boolean'
            else:
                return 'string'
        elif isinstance(field, SetMessageField):
            return 'string'
        else:
            logging.warning('Unknown type for field: %s', field.name)
            return 'string'

    def create_json_field(self, part: DatacastField = None):
        jsonfield = {'title': part.name}
        if isinstance(part, CompositeMessageField):
            jsonfield['type'] = 'object'
            jsonfield['properties'] = {}
            for _, field_part in enumerate(part.parts):
                jsonfield['properties'][field_part.name] = field_part.create_json_field(field_part)
        else:
            jsonfield['type'] = part.get_json_field_type(part)

        return jsonfield

    @staticmethod
    def get_avro_field_type(part: DatacastField = None):
        field = part
        avro_type = ['null', 'string']
        if isinstance(field, TypeMessageField):
            if field.is_bool_type is True:
                avro_type = ['null', 'boolean']  # BQ converts to BOOLEAN
            else:
                mapped_type = avro_type_map[field.primitive_type]
                avro_type = ['null', mapped_type]
        elif isinstance(field, EnumMessageField):
            if field.is_bool_type is True:
                avro_type = ['null', 'boolean']  # BQ converts to BOOLEAN
            else:
                avro_type = ['null', 'string']
        elif isinstance(field, SetMessageField):
            avro_type = ['null', 'string']
        else:
            logging.warning('Unknown type for field: %s', field.name)
        return avro_type

    def create_avro_field(self, part: DatacastField = None):
        field = self
        if part is not None:
            field = part
        if isinstance(field, CompositeMessageField):
            children: [DatacastField] = []
            for _, _part in enumerate(field.parts):
                children.append({'name': _part.name, 'type': SBEMessageField.get_avro_field_type(_part)})
            return {
                'name': field.name,
                'type': {
                    'name': field.name,
                    'type': 'record',
                    'fields': children
                }
            }
        return {'name': field.name, 'type': SBEMessageField.get_avro_field_type(field)}

    @staticmethod
    def get_bigquery_field_type(field: DatacastField = None):
        bq_type = 'STRING'
        if isinstance(field, TypeMessageField):
            if field.is_bool_type is True:
                bq_type = 'BOOLEAN'
            else:
                mapped_type = bigquery_type_map[field.primitive_type]
                bq_type = mapped_type
        elif isinstance(field, EnumMessageField):
            if field.is_bool_type is True:
                bq_type = 'BOOLEAN'
            else:
                bq_type = 'STRING'
        elif isinstance(field, SetMessageField):
            bq_type = 'STRING'
        else:
            logging.warning('Unknown type for field: %s', field.name)
        return bq_type

    def create_bigquery_field(self, part: DatacastField = None):
        field = self
        if part is not None:
            field = part
        if isinstance(field, CompositeMessageField):
            children: [bigquery.SchemaField] = []
            for _, _part in enumerate(field.parts):
                children.append(bigquery.SchemaField(_part.name, SBEMessageField.get_bigquery_field_type(_part)))
            return bigquery.SchemaField(field.name, 'RECORD', mode="NULLABLE", fields=children)
        return bigquery.SchemaField(field.name, SBEMessageField.get_bigquery_field_type(field), mode="NULLABLE")


class TypeMessageField(SBEMessageField):
    def __init__(self, name=None, original_name=None,  # pylint: disable=too-many-arguments
                 id=None, description=None,
                 unpack_fmt=None, field_offset=None,
                 field_length=None, optional=False,
                 null_value=None, constant=None, is_string_type=False,
                 semantic_type=None, since_version=0,
                 primitive_type=None, byte_order: str = None):
        super(SBEMessageField, self).__init__()
        self.name = name
        self.original_name = original_name
        self.id = id
        self.description = description
        self.unpack_fmt = unpack_fmt
        self.field_offset = field_offset
        self.field_length = field_length
        self.optional = optional
        self.null_value = null_value
        self.constant = constant
        self.is_string_type = is_string_type
        self.semantic_type = semantic_type
        self.since_version = since_version
        self.primitive_type = primitive_type
        self.byte_order = byte_order

    def is_int_type(self):
        return 'int' in self.primitive_type

    @property
    def value(self):  # pylint: disable=too-many-return-statements
        _raw_value = self.raw_value

        # Handle nullValues
        if self.primitive_type in null_value:
            n_val = null_value[self.primitive_type]
            if _raw_value == n_val:
                return None

        is_zero_byte_arr = is_empty_byte_array(_raw_value)

        # If this is a null value, return None
        if self.null_value:
            if _raw_value == self.null_value:
                return None
        elif is_zero_byte_arr is True:
            return None

        # If this is a string type, strip any null characters
        if self.is_string_type:
            parts = _raw_value.split(b'\0', 1)
            return parts[0].decode('UTF-8')
        if self.primitive_type == 'char' and not isinstance(_raw_value, str):
            return _raw_value.decode('UTF-8')
        if self.is_bool_type:
            return get_bool_value(_raw_value)

        return _raw_value

    @property
    def raw_value(self):
        if self.constant is not None:
            return self.constant

        start_index = self.msg_offset + self.relative_offset + self.field_offset
        end_index = start_index + self.field_length

        _, primitive_type_size = TypeMap.primitive_type_map[self.primitive_type]
        if self.is_int_type() and primitive_type_size != self.field_length:
            _raw_value = int.from_bytes(self.msg_buffer[start_index: end_index], self.byte_order)
        else:
            _raw_value = unpack_from(self.unpack_fmt, self.msg_buffer, start_index)[0]

        return _raw_value


class SetMessageField(SBEMessageField):
    def __init__(self, name=None, original_name=None, id=None, description=None,  # pylint: disable=too-many-arguments
                 unpack_fmt=None, field_offset=None,
                 choices=None, field_length=None, semantic_type=None, since_version=0):
        super(SBEMessageField, self).__init__()
        self.name = name
        self.original_name = original_name
        self.id = id
        self.description = description
        self.unpack_fmt = unpack_fmt
        self.field_offset = field_offset
        self.choices = choices
        self.field_length = field_length
        self.text_to_name = dict((int(x['text']), x['name']) for x in choices)
        self.semantic_type = semantic_type
        self.since_version = since_version

    @property
    def value(self):
        _raw_value = self.raw_value

        if _raw_value == 0:
            return None

        _value = ''
        _num_values = 0
        for i in range(self.field_length * 8):
            bit_set = 1 & (_raw_value >> i)
            if bit_set:
                if _num_values > 0:
                    _value += ', '
                _value += self.text_to_name[i]
                _num_values += 1
        return _value

    @property
    def raw_value(self):
        _raw_value = unpack_from(self.unpack_fmt, self.msg_buffer,
                                 self.msg_offset + self.relative_offset + self.field_offset)[0]
        return _raw_value


class EnumMessageField(SBEMessageField):
    def __init__(self, name=None, original_name=None, id=None, description=None,  # pylint: disable=too-many-arguments
                 unpack_fmt=None, field_offset=None,
                 enum_values=None, field_length=None, semantic_type=None, since_version=0, enum_fallback_to_name=False,
                 primitive_type=None):
        super(SBEMessageField, self).__init__()
        self.name = name
        self.original_name = original_name
        self.id = id
        self.description = description
        self.unpack_fmt = unpack_fmt
        self.field_offset = field_offset
        self.enum_values = enum_values
        self.field_length = field_length
        self.text_to_enum_description = dict((x['text'], x.get('description', '')) for x in enum_values)
        self.text_to_enumerant = dict((x['text'], x['name']) for x in enum_values)  # shorter repr of value
        self.semantic_type = semantic_type
        self.since_version = since_version
        self.enum_fallback_to_name = enum_fallback_to_name
        self.primitive_type = primitive_type

    @property
    def value(self):
        _raw_value = self.raw_value

        # Handle nullValues
        if self.primitive_type in null_value:
            n_val = null_value[self.primitive_type]
            if _raw_value == n_val:
                return None

        is_zero_byte_arr = is_empty_byte_array(_raw_value)
        if is_zero_byte_arr is True:
            return None

        _value = self.text_to_enum_description.get(str(_raw_value), None)

        if self.enum_fallback_to_name is True and _value is None or _value == '':
            _value = self.text_to_enumerant.get(str(_raw_value), None)

        if self.is_bool_type:
            _value = get_bool_value(_value)

        return _value

    @property
    def enumerant(self):
        _raw_value = self.raw_value
        _enumerant = self.text_to_enumerant.get(str(_raw_value), None)
        return _enumerant

    @property
    def raw_value(self):
        _raw_value = unpack_from(self.unpack_fmt, self.msg_buffer,
                                 self.msg_offset + self.relative_offset + self.field_offset)[0]
        if isinstance(_raw_value, bytes):
            _raw_value = _raw_value.decode('UTF-8')
        return _raw_value


class CompositeMessageField(SBEMessageField):
    def __init__(self, name=None, original_name=None, id=None, description=None,  # pylint: disable=too-many-arguments
                 field_offset=None, field_length=None,
                 parts=None, float_value=False, semantic_type=None, since_version=0):
        super(SBEMessageField, self).__init__()
        self.name = name
        self.original_name = original_name
        self.id = id
        self.description = description
        self.field_offset = field_offset
        self.field_length = field_length
        self.parts = parts
        self.float_value = float_value
        self.semantic_type = semantic_type
        self.since_version = since_version

        # Map the parts
        for part in self.parts:
            setattr(self, part.name, part)

    def wrap(self, msg_buffer, msg_offset, relative_offset=0):
        self.msg_buffer = msg_buffer
        self.msg_offset = msg_offset
        self.relative_offset = relative_offset

        for part in self.parts:
            part.wrap(msg_buffer, msg_offset, relative_offset=relative_offset)

    @property
    def value(self):
        _raw_value = self.raw_value

        # if self.float_value:
        #     # We expect two fields, mantissa and exponent as part of this field
        #     mantissa = _raw_value.get('mantissa', None)
        #     exponent = _raw_value.get('exponent', None)
        #     if mantissa is None or exponent is None:
        #         return None
        #     return float(mantissa) * math.pow(10, exponent)

        return self.raw_value

    @property
    def raw_value(self):
        part_dict = dict((p.name, p.value) for p in self.parts)
        return part_dict


class SBERepeatingGroup:
    def __init__(self, msg_buffer, msg_offset, relative_offset, name, original_name, fields):
        self.msg_buffer = msg_buffer
        self.msg_offset = msg_offset
        self.relative_offset = relative_offset
        self.fields = fields
        self._groups = []
        self.name = name
        self.original_name = original_name

        for field in fields:
            setattr(self, field.name, field)

    def wrap(self):
        for field in self.fields:
            field.wrap(self.msg_buffer, self.msg_offset, relative_offset=self.relative_offset)

    def add_subgroup(self, subgroup):
        if not hasattr(self, subgroup.name):
            setattr(self, subgroup.name, [subgroup])
        else:
            getattr(self, subgroup.name).append(subgroup)
        self._groups.append(subgroup)

    @property
    def groups(self):
        for group in self._groups:
            group.wrap()
            yield group


class SBERepeatingGroupContainer:
    def __init__(self, name=None, original_name=None, id=None,  # pylint: disable=too-many-arguments
                 block_length_field=None, num_in_group_field=None, dimension_size=None, fields=None,
                 groups=None, since_version=0):
        self.msg_buffer = None
        self.msg_offset = 0
        self.group_start_offset = 0

        self.name = name
        self.original_name = original_name
        self.id = id
        self.block_length_field = block_length_field
        self.num_in_group_field = num_in_group_field

        if fields is None:
            self.fields = []
        else:
            self.fields = fields

        if groups is None:
            self.groups = []
        else:
            self.groups = groups
        self.since_version = since_version

        self.dimension_size = dimension_size
        self._repeating_groups = []

    def wrap(self, msg_buffer, msg_offset, group_start_offset):
        self.msg_buffer = msg_buffer
        self.msg_offset = msg_offset
        self.group_start_offset = group_start_offset
        self.block_length_field.wrap(msg_buffer, msg_offset, relative_offset=group_start_offset)
        self.num_in_group_field.wrap(msg_buffer, msg_offset, relative_offset=group_start_offset)
        block_length = self.block_length_field.value
        num_instances = self.num_in_group_field.value

        self._repeating_groups = []

        # TODO: Can this be removed?
        self.group_offset = group_start_offset + self.dimension_size

        # for each group, add the group length which can vary due to nested groups
        repeated_group_offset = group_start_offset + self.dimension_size
        nested_groups_length = 0
        for _ in range(num_instances):
            repeated_group = SBERepeatingGroup(msg_buffer,
                                               msg_offset,
                                               repeated_group_offset + nested_groups_length,
                                               self.name,
                                               self.original_name,
                                               self.fields)
            self._repeating_groups.append(repeated_group)
            repeated_group_offset += block_length
            # now account for any nested groups
            for nested_group in self.groups:
                nested_groups_length += nested_group.wrap(
                    msg_buffer, msg_offset, repeated_group_offset + nested_groups_length)
                for nested_repeating_group in nested_group._repeating_groups:  # pylint: disable=protected-access
                    repeated_group.add_subgroup(nested_repeating_group)

        size = self.dimension_size + (num_instances * block_length) + nested_groups_length
        return size

    @property
    def num_groups(self):
        return len(self._repeating_groups)

    @property
    def repeating_groups(self):
        for group in self._repeating_groups:
            group.wrap()
            yield group

    def __getitem__(self, index):
        group = self._repeating_groups[index]
        group.wrap()
        return group


class SBEMessage:
    def __init__(self):
        self.name = self.__class__.__name__
        self.msg_buffer = None
        self.msg_offset = None

    @staticmethod
    def parse_message(schema, msg_buffer, offset=0):
        """ Return a message by parsing a msg_buffer with the specified schema """
        template_id_offset = 2  # the 2 byte BlockHeader that starts all SBE Messages
        if schema.include_message_size_header:
            template_id_offset = 4  # Include a two byte message header (i.e for CME MDP)
        template_id = unpack_from('<H', msg_buffer, offset + template_id_offset)[0]
        message_type = schema.get_message_type(template_id)
        message = message_type()
        message.wrap(msg_buffer, offset)
        return message

    def wrap(self, msg_buffer, msg_offset):
        # Wrap the fields for decoding
        self.msg_buffer = msg_buffer
        self.msg_offset = msg_offset

        message_version = 0
        for field in self.fields:
            if field.since_version > message_version > 0:
                continue
            field.wrap(msg_buffer, msg_offset)
            if field.name == 'version':  # as we're iterating fields, save the version, which comes early as part of header
                message_version = field.value

        # Wrap the groups for decoding
        group_offset = self.schema_block_length + self.header_size
        for group in self.groups:
            if group.since_version <= message_version:
                group_offset += group.wrap(msg_buffer, msg_offset, group_offset)

    def __str__(self):
        return self.__class__.__name__


class SBEMessageFactory:  # pylint: disable=too-few-public-methods
    def __init__(self, schema):
        self.schema = schema

    # This should return a tuple of (message, message_size)
    def build(self, msg_buffer, offset):
        raise NotImplementedError()
