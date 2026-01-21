#
# Copyright 2023 Google LLC
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

__version__ = '1.0.4'

from importlib.metadata import version, PackageNotFoundError

from .Transcoder import Transcoder

try:
    # This must match the 'name' field in your pyproject.toml
    __version__ = version("market-data-transcoder")
except PackageNotFoundError:
    # This handles the case where the package is not installed
    # (e.g. running scripts directly from the source folder)
    __version__ = "unknown"
