#!/bin/bash
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

OUTPUT_TYPE=${1}

[ -z ${OUTPUT_TYPE} ] && OUTPUT_TYPE=diag

pushd ../..

wget -q -O - ftp://ftp.cmegroup.com/SBEFix/Production/secdef.dat.gz|gunzip - | head -10 > secdef.dat
wget -q -O - ftp://ftp.cmegroup.com/SBEFix/Production/TradingSessionList.dat| head -10 >> secdef.dat
wget -q 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX50SP2.CME.xml'

python3 main.py \
  --source_file secdef.dat \
  --schema_file FIX50SP2.CME.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --continue_on_error \
  --output_type ${OUTPUT_TYPE} \
  --message_type_inclusions SecurityDefinition,TradingSessionList

rm secdef.dat FIX50SP2.CME.xml

popd

