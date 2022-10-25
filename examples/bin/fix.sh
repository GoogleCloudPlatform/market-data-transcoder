#!/bin/bash

pushd ../..

wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/testfiles/42_order_single.txt'
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX42.xml'

python3 main.py \
  --source_file 42_order_single.txt \
  --schema_file ../fix2json/dict/FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --continue_on_error \
  --message_type_inclusions NewOrderSingle

rm 42_order_single.txt FIX42.xml

popd

