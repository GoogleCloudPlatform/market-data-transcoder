"""
This module defines a python representation of a Fix Specification.
Most of the content of this module is opaque to the casual user. but is useful to customise
The typical use is to instantiate FixSpec with a file from
the spec according to one's requirements.

This module uses xml parsing logic intensively, so we recommend having lxml (lxml.de) installed
to speed it up. It will work with the python-shipped xml module as well, although will be slower.

Note::
   this module doesn't (yet) support hops as part of a message header (in FIX4.4 onwards)

"""

# pylint: skip-file

from google.cloud import bigquery

from transcoder.message.DatacastField import DatacastField

try:
    from lxml.etree import Comment, parse
except ImportError:
    from xml.etree.ElementTree import Comment, parse  # pylint: disable=C0411

# FIX50 onwards don't define the header & trailer. We still need something to order the tags if
# they end up on messages, so we'll use this instead. This doesn't support the Hops component
# for simplicity's sake for now.
HEADER_TAGS = [8, 9, 35, 1128, 1156, 1129, 49, 56, 115, 128,
               90, 91, 34, 50, 142, 57, 143, 116, 144, 129, 145,
               43, 97, 52, 122, 212, 213, 347, 369, 370, 1128, 1129]
TRAILER_TAGS = [93, 89, 10]
ENCODED_DATA_TAGS = [349, 351, 353, 355, 357, 359, 361, 363, 365]
HEADER_SORT_MAP = {t: i for i, t in enumerate(HEADER_TAGS)}
HEADER_SORT_MAP.update({10: int(10e9), 89: int(10e9 - 1), 93: int(10e9 - 2)})

BOOLEAN_TYPES = ['boolean']
INTEGER_TYPES = ['int', 'dayofmonth', 'length', 'seqnum']
FLOAT_TYPES = ['float', 'qty', 'price', 'priceoffset', 'amt', 'percentage']
STRING_TYPES = ['string', 'currency', 'exchange', 'multiplevaluestring', 'char', 'utctimestamp', 'utctimeonly',
                'localmktdate', 'utcdate', 'monthyear', 'data', 'country', 'multiplevaluestring']


class FixTag(DatacastField):
    """
    Fix tag representation. A fix tag has name, tag (number), type and valid values (enum)
    """

    def __init__(self, name, tag, tagtype=None, values=tuple()):
        """
        :param name: Tag name
        :type name: ``str``
        :param tag: Tag number
        :type tag: ``int``
        :param tagtype: Type as in quickfix's XML reference documents
        :type tagtype: ``str``
        :param values:  The valid enum values
        :type values: ``tuple((str, str))``  with the first element of each tuple being the value of the enum,
          the second the name of the value.
        """
        self.name = name
        self.tag = tag
        self.type = tagtype
        self._values = values
        self._is_enum = len(values) > 0
        self._val_by_name = {}
        self._val_by_val = {}

    def add_enum_value(self, name, value):
        """Add a value to the tag's enum values."""
        if name in set(v[1] for v in self._values):
            raise KeyError(f'Name {name} is already known in tag {self.tag}\'s enum')
        self._values = self._values + ((value, name),)
        if self._val_by_name:
            self._val_by_name[name] = value
        if self._val_by_val:
            self._val_by_val[value] = name

    def del_enum_value(self, name=None, value=None):
        """Delete a value from the tag's enum values.
        Specify name or value using keyword arguments. If specifying both, they must both match the known
        name and value otherwise ValueError is raised.
        """
        if name is None and value is None:
            raise TypeError("either name or value is required")
        if name and value:
            if self._val_by_name[name] != value:
                raise ValueError(f'The known value {self._val_by_name[name]} for enum name {name} is different to you '
                                 f'gave: {value} for tag {self.tag}')
        if name:
            if name not in set(v[1] for v in self._values):
                # can't use the maps here because when deleting multiple tags they are empty come the second
                raise KeyError(f'{name} is not known as a name for tag {self.tag}')
            self._values = tuple(pair for pair in self._values if pair[1] != name)
        else:
            if value not in set(v[0] for v in self._values):
                raise KeyError(f'{value} is not known as a value for tag {self.tag}')
            self._values = tuple(pair for pair in self._values if pair[0] != value)

        self._val_by_name = {}
        self._val_by_val = {}

    def enum_by_name(self, name):
        """ Retrieve an enum value by name"""
        if not self._val_by_name:
            self._val_by_name = {name: val for val, name in self._values}
        return self._val_by_name[name]

    def enum_by_value(self, value):
        """ Retrieve an enum value by value"""
        if not self._val_by_val:
            self._val_by_val = dict(self._values)
        return self._val_by_val[value]

    def create_json_field(self, part: DatacastField = None):
        obj = {}
        obj[self.name] = {}
        obj[self.name]['type'] = self.get_json_field_type()
        obj[self.name]['title'] = self.name
        return obj

    def get_json_field_type(self):
        _type = self.type.lower()
        json_type = None
        if self._is_enum is True or _type in STRING_TYPES:
            json_type = 'string'
        elif _type in INTEGER_TYPES:
            json_type = 'integer'
        elif _type in FLOAT_TYPES:
            json_type = 'number'
        elif _type in BOOLEAN_TYPES:
            json_type = 'boolean'

        return json_type

    def get_avro_field_type(self):
        _type = self.type.lower()
        avro_type = ['null', 'string']
        if self._is_enum is True or _type in STRING_TYPES:
            avro_type = ['null', 'string']
        elif _type in INTEGER_TYPES:
            avro_type = ['null', 'int']
        elif _type in FLOAT_TYPES:
            avro_type = ['null', 'float']
        elif _type in BOOLEAN_TYPES:
            avro_type = ['null', 'boolean']
        return avro_type

    def create_avro_field(self, part: DatacastField = None):
        return {'name': self.name, 'type': self.get_avro_field_type()}

    def cast_value_to_type(self, value, field_type: str, is_nullable: bool = True):
        result = value
        _type = field_type.lower()
        if self._is_enum is True:
            result = self.enum_by_value(value)
        elif _type in STRING_TYPES:
            result = str(value)
        elif _type in INTEGER_TYPES:
            result = int(value)
        elif _type in FLOAT_TYPES:
            result = float(value)
        elif _type in BOOLEAN_TYPES:
            result = bool(value)
        else:
            result = str(value)
        return result

    def get_bigquery_field_type(self):
        _type = self.type.lower()
        bq_type = 'STRING'
        if self._is_enum is True or _type in STRING_TYPES:
            bq_type = 'STRING'
        elif _type in INTEGER_TYPES:
            bq_type = 'NUMERIC'
        elif _type in FLOAT_TYPES:
            bq_type = 'FLOAT'
        elif _type in BOOLEAN_TYPES:
            bq_type = 'BOOLEAN'
        return bq_type

    def create_bigquery_field(self, part: DatacastField = None):
        return bigquery.SchemaField(self.name, self.get_bigquery_field_type(), mode="NULLABLE")

    def is_equal(self, other):
        is_fix_tag = isinstance(other, self.__class__)
        if not is_fix_tag:
            return False
        return self.name == other.name and \
               self.tag == other.tag and \
               self.type == other.type

    def __repr__(self):
        return f'FixTag(name: {self.name}, tag: {self.tag}, type: {self.type})'


class TagsReference:
    """ Container for tags with maps by name and tag"""

    def __init__(self, tags, eager=False):
        """
        :param tags: set of FixTag objects
        :param eager: whether to create the mapping by name and tag
        immediately or upon the first use of them
        """
        self.tags = tags
        self._by_name = {}
        self._by_tag = {}
        if eager:
            try:
                self.by_name(None)
            except KeyError:
                pass
            try:
                self.by_tag(None)
            except KeyError:
                pass

    def add_tag(self, tag, name):
        """Add a tag to the list of valid tags"""
        tag_inst = FixTag(name=name, tag=tag)
        self.tags.add(tag_inst)
        if self._by_name:
            self._by_name[name] = tag_inst
        if self._by_tag:
            self._by_tag[tag] = tag_inst

    def by_name(self, name):
        """Retrieve a tag by name"""
        if not self._by_name:
            self._by_name = {t.name: t for t in self.tags}
        return self._by_name[name]

    def by_tag(self, tag):
        """
        Retrieve a tag by number.
        """
        if not self._by_tag:
            self._by_tag = {t.tag: t for t in self.tags}
        return self._by_tag[tag]


class FixSpec:  # pylint: disable=too-few-public-methods
    """
    A python-friendly representation of a FIX spec.
    This class is built from an XML file sourced from Quickfix (http://www.quickfixengine.org/).

    It contains the Message Types supported by the specification, as a map (FixSpec.msg_types) of
    message type value ('D', '6', '8', etc..) to MessageType class, and all the fields supported
    in the spec as a TagReference instance (FixSpec.tags) which can be accessed by tag name or number.
    """

    def __init__(self, xml_file, eager=False):
        """
        :param xml_file: path to a quickfix specification xml file
        :type xml_file: ``str``
        :param eager: whether to eagerly populate tags maps for speedy lookup or only on first access
        :type eager: ``bool``
        """
        self.source = xml_file
        self.tree = parse(xml_file).getroot()
        major = self.tree.get('major')
        minor = self.tree.get('minor')
        self.version = f'FIX{major}.{minor}'
        self._eager = eager
        self.tags = None
        self._populate_tags()
        self.msg_types = {m.msgtype: m for m in
                          (MessageType(e, self) for e in
                           self.tree.findall('messages/message'))}
        # We need to be able to look msg type for both decoded and raw values of tag 35
        msg_type_list = list(self.msg_types.items())
        self.msg_types.update(
            {key.encode('ascii'): val for key, val in msg_type_list})
        self.header_tags = [self.tags.by_name(t.get('name')) for t in self.tree.findall('header/field')]
        self.trailer_tags = [self.tags.by_name(t.get('name')) for t in self.tree.findall('trailer/field')]
        self.tree = None

    def _populate_tags(self):
        """populate the TagReference from the xml file"""
        tags = set()
        for field in self.tree.findall('fields/field'):
            enums = tuple(e.get('enum') for e in field.findall('value'))
            descriptions = tuple(e.get('description') for e in field.findall('value'))
            values = tuple(zip(enums, descriptions))
            tag = FixTag(field.get('name'), int(field.get('number')), field.get('type'), values)
            tags.add(tag)
        self.tags = TagsReference(tags, self._eager)


def _extract_composition(element, spec):
    """
    Parse XML spec to extract the composition of a nested structure (Component, Group or MsgType)
    """
    returned = []
    for elem in list(element):
        if elem.tag == "field":
            returned.append((spec.tags.by_name(elem.get('name')),
                             elem.get('required') == "Y"))
        elif elem.tag == 'component':
            returned.append((Component(elem, spec), elem.get('required') == "Y"))
        elif elem.tag == 'group':
            returned.append((Group.from_element(elem, spec), elem.get('required') == "Y"))
        elif parse.__module__ == 'lxml.etree' and elem.tag == Comment():
            pass
        else:
            raise ValueError(f'Could not process element \'{elem.tag}\'')
    return returned


def _get_groups(composition):
    """ Recursively extract groups from a composition"""
    for item, _ in composition:
        if isinstance(item, Group):
            yield item
        elif isinstance(item, Component):
            for group in _get_groups(item.composition):
                yield group


class Group:
    """
    Representation of the specification of a Repeating Group.
    """

    def __init__(self, count_tag, composition, spec):
        """
        :param count_tag: A FixTag object representing a repeating group. Must correspond
          to the tag name for its count tag in the spec.
        :type count_tag: ``FixTag``
        :param composition: the xml elements representing a fix tag, a component or a group
        :type composition: ``list`` of ``etree.Element``
        :param spec: the (partially populated) specification, containing at least the components
          and tags.
        :type spec: ``FixSpec``
        """
        self.composition = composition
        self.count_tag = count_tag
        self.name = count_tag.name
        self.tags = set(t[0].tag for t in self.composition if isinstance(t[0], FixTag))
        self.all_child_tags = self.parse_child_tags(self.composition)
        self.groups = {group.count_tag.tag: group for group in _get_groups(self.composition)}
        self._sorting_key = None
        self._spec = spec

    def parse_child_tags(self, composition):
        tags = set()
        for element, _ in composition:
            if isinstance(element, FixTag):
                if element.tag not in tags:
                    tags.add(element.tag)
            elif isinstance(element, Component):
                tags.update(self.parse_child_tags(element.composition))
        return tags

    @property
    def sorting_key(self):
        if not self._sorting_key:
            self._sorting_key = _extract_sorting_key(self.composition, self._spec)
        return self._sorting_key

    @classmethod
    def from_element(cls, element, spec):
        """Build the group from an lxml etree element
        :param element: the xml element representing the group
        :type element: ``etree.Element``
        :param spec: a :py:class:`FixSpec` describing the element
        :type spec: ``FixSpec``
        """
        return cls(count_tag=spec.tags.by_name(element.get('name')),
                   composition=_extract_composition(element, spec), spec=spec)

    def add_group(self, count_tag, composition, insert_at=None):
        """Add a synthetic group to this group.
        You may need to add it to the relevant message type as well!"""
        group = Group(count_tag=count_tag, composition=composition, spec=self._spec)
        self.groups[count_tag.tag] = group
        if insert_at:
            self.sorting_key[count_tag.tag] = insert_at
            # Will sort by tag number after the sorted tags otherwise


class Component:  # pylint: disable=too-few-public-methods
    """Representation of the specification of a Component"""

    def __init__(self, element, spec):
        """
        :param element: the xml element representing a component
        :type element: ``etree.Element``
        :param spec: the (partially populated) specification, containing at least the groups
          and tags
        """
        self.name = element.get('name')
        elem = spec.tree.findall(f"components/component[@name='{self.name}']")[0]
        self.composition = _extract_composition(elem, spec)
        self._sorting_key = None
        self._spec = spec

    @property
    def sorting_key(self):
        if not self._sorting_key:
            self._sorting_key = _extract_sorting_key(self.composition, self._spec)
        return self._sorting_key


class MessageType:
    """
    Message Type representation. Contains the valid tags, their order, valid repeating groups,
    components etc.
    """

    def __init__(self, element, spec):
        """
        :param element: the xml element representing a message type
        :type element: ``etree.Element``
        :param spec: the (partially populated) specification, containing at least the tags.
        :type spec: ``FixSpec``
        """
        assert element.tag == "message"
        self.msgtype = element.get('msgtype')
        self.name = element.get('name')
        self.composition = _extract_composition(element, spec)
        self.groups = {group.count_tag.tag: group for group in _get_groups(self.composition)}
        self._sorting_key = None
        self._spec = spec

    @property
    def sorting_key(self):
        if not self._sorting_key:
            self._sorting_key = _extract_sorting_key(self.composition, self._spec)
        return self._sorting_key

    def add_group(self, count_tag, composition, insert_at=None):
        """Add a synthetic group to this msg type.
        You may need to add it to the relevant message type as well!"""
        group = Group(count_tag=count_tag, composition=composition, spec=self._spec)
        self.groups[count_tag.tag] = group
        if insert_at:
            self.sorting_key[count_tag.tag] = insert_at
            # Will sort by tag number after the sorted tags otherwise

    def __repr__(self):
        return f'MessageType(name: {self.name})'


def _extract_sorting_key(definition, spec, sorting_key=None, index=0):
    """
    Retrieve the sorting key for an object.
    The sorting key is used to serialise tags in the order they appear in the spec.
    It is unclear whether that's required for the root of the message (aside from header and tail)
    but it is essential in repeating groups. This takes the safe approach of enforcing it at all
    levels.
    """
    if sorting_key is None:
        sorting_key = {35: 0, 10: int(10e9)}
        trailer_tags = [item.tag for item in spec.trailer_tags] or TRAILER_TAGS
        for i, item in enumerate(trailer_tags[::-1]):
            sorting_key[item] = 10e9 - i
        header_tags = [item.tag for item in spec.header_tags] or HEADER_TAGS
        for i, item in enumerate(header_tags):
            sorting_key[item] = i

    start_index = index + 1
    for i, (item, _) in enumerate(definition):
        if isinstance(item, FixTag):
            sorting_key[item.tag] = i + start_index
        elif isinstance(item, Component):
            _extract_sorting_key(item.composition, spec, sorting_key, index=i + start_index)
        elif isinstance(item, Group):
            sorting_key[item.count_tag.tag] = i + start_index

    return sorting_key
