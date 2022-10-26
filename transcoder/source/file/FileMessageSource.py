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
import logging
from io import IOBase

from transcoder.source.Source import Source, SourceFunctionNotDefinedError


class FileMessageSource(Source):
    """Abstract file message source class"""

    @staticmethod
    def source_type_identifier():
        raise SourceFunctionNotDefinedError

    def __init__(self, file_path: str):
        super().__init__()
        self.path = file_path
        self.file_handle: IOBase = None
        self.file_size = 0
        self.log_percentage_read_enabled = logging.getLogger().isEnabledFor(logging.DEBUG)

    def open(self):
        raise SourceFunctionNotDefinedError

    def close(self):
        self.file_handle.close()

    def get_message_iterator(self):
        raise SourceFunctionNotDefinedError

    def _log_percentage_read(self):
        if self.log_percentage_read_enabled is True:
            logging.debug('Percentage read: %f%%', round((self.file_handle.tell() / self.file_size) * 100, 6))
