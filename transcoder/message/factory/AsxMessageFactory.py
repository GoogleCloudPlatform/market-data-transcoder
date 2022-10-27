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

import struct

from third_party.sbedecoder import SBEMessageFactory
from transcoder.message.factory.exception import TemplateSchemaNotDefinedError


class AsxMessageFactory(SBEMessageFactory):  # pylint: disable=too-few-public-methods
    """ASX-specific logic to unpack message from buffer & decode according to message template"""

    def build(self, msg_buffer, offset):
        message_type_str = struct.unpack_from('>c', msg_buffer, offset)[0]
        template_id = ord(message_type_str)
        message_type = self.schema.get_message_type(template_id)

        if message_type is None:
            raise TemplateSchemaNotDefinedError(f'Schema not found for template_id: {template_id} supporting '
                                                f'message_type: {message_type_str.decode()}')

        message = message_type()
        message.wrap(msg_buffer, offset)
        return message, len(msg_buffer)
