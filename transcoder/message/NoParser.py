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

from .DatacastParser import DatacastParser


class NoParser(DatacastParser):
    """ NOOP parser that simply maintains a record count. Intended to be used with the frame-only option. """

    # pylint: disable=super-init-not-called

    def __init__(self):
        self.record_count = 0
        self.summary_count = 0

    # pylint: disable=unused-argument
    def process_message(self, raw_msg=None):
        """ Update message counter """
        self.record_count += 1
        self.summary_count += 1
        return {}
