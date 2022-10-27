#!/bin/bash

pushd ../..

wget 'ftp://ftp.cmegroup.com/SBEFix/Production/Templates/templates_FixBinary_v12.xml'
wget 'https://github.com/Open-Markets-Initiative/Data/blob/main/Cme/Mdp3.Sbe.v1.12/SnapshotFullRefreshTcpLongQty.68.Tcp.pcap?raw=true' -O SnapshotFullRefreshTcpLongQty.pcap

python3 main.py \
  --source_file SnapshotFullRefreshTcpLongQty.pcap  \
  --schema_file templates_FixBinary_v12.xml \
  --factory cme \
  --source_file_format_type pcap \
  --message_skip_bytes 16 \
  --message_type_inclusions SnapshotFullRefreshTCPLongQty68

rm SnapshotFullRefreshTcpLongQty.pcap templates_FixBinary_v12.xml

popd
