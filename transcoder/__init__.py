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

from enum import Enum


# pylint: disable=invalid-name

class LineEncoding(Enum):
    """Line encoding types supported for individual message decoding before processing"""
    NONE = 0
    BASE_64 = 1
    BASE_64_URL_SAFE = 2

    @classmethod
    def _missing_(cls, value):
        return LineEncoding.NONE

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name
