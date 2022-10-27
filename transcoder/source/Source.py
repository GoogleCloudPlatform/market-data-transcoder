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

class Source:
    """Class representing sources of market data"""

    @staticmethod
    def source_type_identifier():
        """Returns identifier of source type"""
        raise SourceFunctionNotDefinedError

    def __init__(self):
        self.record_count = 0

    def open(self) -> bool:
        """Opens source of market data"""
        raise SourceFunctionNotDefinedError

    def close(self) -> bool:
        """Closes source of market data"""
        raise SourceFunctionNotDefinedError

    def get_message_iterator(self):
        """Returns message iterator specific to source"""
        raise SourceFunctionNotDefinedError

    def increment_count(self):
        """Increments count of messages"""
        self.record_count += 1

    def __enter__(self):
        return self.open()

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()


class SourceFunctionNotDefinedError(Exception):
    """Exception thrown by subclasses not overriding base class methods"""
