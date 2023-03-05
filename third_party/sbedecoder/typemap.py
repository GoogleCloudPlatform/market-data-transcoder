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

class TypeMap:  # pylint: disable=too-few-public-methods)
    """Type map for primitive types to unpack fmt and length"""
    primitive_type_map = {
        'char': ('c', 1),
        'int': ('i', 4),
        'int8': ('b', 1),
        'int16': ('h', 2),
        'int32': ('i', 4),
        'int64': ('q', 8),
        'uint8': ('B', 1),
        'uint16': ('H', 2),
        'uint32': ('I', 4),
        'uint64': ('Q', 8),
        'float': ('f', 4),
        'double': ('d', 8),
    }
