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

from struct import unpack_from

from third_party.sbedecoder import SBEMessageFactory
# Extracted and re-factored from SBEDecoder
from transcoder.message.factory.exception import TemplateSchemaNotDefinedError


class MDPMessageFactory(SBEMessageFactory):  # pylint: disable=too-few-public-methods,duplicate-code
    """MDP-specific logic to unpack message from buffer & decode according to message template"""

    def build(self, msg_buffer, offset):
        # Peek at the template id to figure out what class to build.
        # This looks past the starting 2 byte MsgSize header that is CME specific
        # and the 2 byte BlockLength that starts all SBE Messages:
        #   https://www.cmegroup.com/confluence/display/EPICSANDBOX/MDP+3.0+-+Message+Header
        template_id = unpack_from('<H', msg_buffer, offset + 4)[0]
        message_type = self.schema.get_message_type(template_id)

        if message_type is None:
            raise TemplateSchemaNotDefinedError(f'Schema not found for template_id: {template_id}')

        message = message_type()
        message.wrap(msg_buffer, offset)
        return message, message.message_size.value
