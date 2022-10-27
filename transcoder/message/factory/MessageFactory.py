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

from third_party.sbedecoder import SBESchema, SBEMessageFactory
from transcoder.message.factory import AsxMessageFactory, CmeMessageFactory, MemxMessageFactory
from transcoder.message.factory.exception.FactoryNotFoundError import FactoryNotFoundError


def get_message_factory(name: str, schema_file_path: str) -> SBEMessageFactory:
    """Gets a user-specified factory with the parsed schema"""
    schema = SBESchema(enum_fallback_to_name=True, include_constants_in_offset=False)
    schema.parse(schema_file_path)
    factory: SBEMessageFactory = None

    if name == 'asx':
        factory = AsxMessageFactory(schema)
    elif name == 'cme':
        factory = CmeMessageFactory(schema)
    elif name == 'memx':
        factory = MemxMessageFactory(schema)
    else:
        raise FactoryNotFoundError(f'Factory with name "{name}" is not valid')

    return factory
