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

wget 'ftp://ftp.cmegroup.com/SBEFix/Production/Templates/templates_FixBinary_v12.xml'
wget 'https://github.com/Open-Markets-Initiative/Data/blob/main/Cme/Mdp3.Sbe.v1.12/SnapshotFullRefreshTcpLongQty.68.Tcp.pcap?raw=true' -O SnapshotFullRefreshTcpLongQty.pcap

python3 main.py \
  --source_file SnapshotFullRefreshTcpLongQty.pcap  \
  --schema_file templates_FixBinary_v12.xml \
  --factory cme \
  --source_file_format_type pcap \
  --message_skip_bytes 16 \
  --output_type ${OUTPUT_TYPE} \
  --message_type_inclusions SnapshotFullRefreshTCPLongQty68

rm SnapshotFullRefreshTcpLongQty.pcap templates_FixBinary_v12.xml

popd
