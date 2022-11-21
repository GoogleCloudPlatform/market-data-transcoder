# Transcoder examples

*_Note_: Some of the examples below assume that [gcloud](https://cloud.google.com/sdk/docs/install), [jq](https://stedolan.github.io/jq/download/), [wget](https://www.gnu.org/software/wget/), and [java](https://www.java.com/en/download/manual.jsp) are installed on the system.*

## Ingest the contents of a CME Group Datamine packet capture file into BigQuery

This example extracts and transcodes messages embedded within a CME Group Datamine packet capture file. A CME Group SBE schema is used to decode the messages. Console output of decoded messages is suppressed. The schema and output files are written to BigQuery. Please note that usage of CME Group data is subject to your licensing agreement with CME Group.


### CLI

```
bin/txcode  \
  --factory cme \
  --schema_file templates_FixBinary_v12.xml \
  --source_file 314-20221007-e \
  --source_file_format_type cme_binary_packet \
  --continue_on_error \
  --message_skip_bytes 2 \
  --source_file_endian little \
  --output_type bigquery \
  --quiet \
  --message_type_inclusions MDIncrementalRefreshBook46 \
  --destination_project_id $(gcloud config get-value project) \
  --destination_dataset_id datamine \
  --lazy_create_resources
```

### Show table in BigQuery

```
bq ls datamine
```

### Output

```
           tableId             Type    Labels   Time Partitioning   Clustered Fields  
 ---------------------------- ------- -------- ------------------- ------------------ 
  MDIncrementalRefreshBook46   TABLE      
```

### Sample records from BigQuery
```
bq --format=json head datamine.MDIncrementalRefreshBook46 | head -1 | jq .[0]
```

### Output

```
{
  "match_event_indicator": "LastQuoteMsg",
  "no_md_entries": [
    {
      "md_entry_px": {
        "exponent": "-9",
        "mantissa": "11173000000000"
      },
      "md_entry_size": "43",
      "md_entry_type": "Offer",
      "md_price_level": "7",
      "md_update_action": "Change",
      "number_of_orders": "16",
      "rpt_seq": "6011055",
      "security_id": "190915",
      "tradeable_size": "0"
    }
  ],
  "no_order_id_entries": [
    {
      "md_display_qty": "1",
      "md_order_priority": "353701821378.0",
      "order_id": "8813654790160.0",
      "order_update_action": "Update",
      "reference_id": "1"
    }
  ],
  "transact_time": "1.6651188000004639e+18"
}
```

## Process messages from a packet capture file (PCAP)

This example extracts and transcodes a message embedded within a PCAP file. A CME Group SBE schema is used to decode the message. The first 16 bytes of the message payload is stripped off before the message is manufactured by and returned from the message factory. The transcoded message is displayed on the console encoded in YAML. Please note that usage of CME Group data is subject to your licensing agreement with CME Group.

### CLI

```
# download SBE template XML
wget 'ftp://ftp.cmegroup.com/SBEFix/Production/Templates/templates_FixBinary_v12.xml'

# download example PCAP to local file system
wget 'https://github.com/Open-Markets-Initiative/Data/blob/main/Cme/Mdp3.Sbe.v1.12/SnapshotFullRefreshTcpLongQty.68.Tcp.pcap?raw=true' -O SnapshotFullRefreshTcpLongQty.pcap

# transcode message, display on console only
bin/txcode  \
    --factory cme \
    --schema_file templates_FixBinary_v12.xml \
    --source_file SnapshotFullRefreshTcpLongQty.pcap \
    --source_file_format_type pcap \
    --message_skip_bytes 16 \
    --message_type_inclusions SnapshotFullRefreshTCPLongQty68
```

### Output

```
high_limit_price:
  exponent: -9
  mantissa: null
low_limit_price:
  exponent: -9
  mantissa: 50000
match_event_indicator: EndOfEvent
max_price_variation:
  exponent: -9
  mantissa: 3000000
no_md_entries:
- md_entry_px:
    exponent: -9
    mantissa: 1183650000
  md_entry_size: 1000000
  md_entry_type: Market Best Bid
  md_price_level: 1
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1183750000
  md_entry_size: 1000000
  md_entry_type: Market Best Offer
  md_price_level: 1
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1183650000
  md_entry_size: 1000000
  md_entry_type: Bid
  md_price_level: 1
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1180000000
  md_entry_size: 1000000
  md_entry_type: Bid
  md_price_level: 2
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1183750000
  md_entry_size: 1000000
  md_entry_type: Offer
  md_price_level: 1
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1207900000
  md_entry_size: 2000000
  md_entry_type: Offer
  md_price_level: 2
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1208000000
  md_entry_size: 2000000
  md_entry_type: Offer
  md_price_level: 3
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1208050000
  md_entry_size: 17000000
  md_entry_type: Offer
  md_price_level: 4
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1208100000
  md_entry_size: 13000000
  md_entry_type: Offer
  md_price_level: 5
  number_of_orders: null
  open_close_settl_flag: null
- md_entry_px:
    exponent: -9
    mantissa: 1207300000
  md_entry_size: 0
  md_entry_type: Trade
  md_price_level: null
  number_of_orders: null
  open_close_settl_flag: null
security_id: 11827320
transact_time: 1631080313799842417

INFO:root:Message type inclusions: ['SnapshotFullRefreshTCPLongQty68']
INFO:root:Source record count: 1
INFO:root:Processed record count: 1
INFO:root:Processed schema count: 1
INFO:root:Summary of message counts: {'SnapshotFullRefreshTCPLongQty68': 1}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 127.000254 per second
INFO:root:Total runtime in seconds: 0.007874
INFO:root:Total runtime in minutes: 0.000131
```

## Publish FIX messages stored in a file to Pub/Sub as Avro binary

This example reads a file containing a single FIX message per line and a FIX schema XML file, creates topics in Pub/Sub for a single message type, then publishes each message in the file to the corresponding topic. A JSON representation of the transcoded message is sent to the console.

### CLI

```
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/testfiles/42_order_single.txt'
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX42.xml'
bin/txcode \
  --source_file 42_order_single.txt \
  --schema_file FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_type pubsub \
  --lazy_create_resources \
  --message_type_inclusions NewOrderSingle
```

### Output

```
INFO:root:Message type inclusions: ['NewOrderSingle']
INFO:root:Source record count: 1
INFO:root:Processed record count: 1
INFO:root:Processed schema count: 2
INFO:root:Summary of message counts: {'NewOrderSingle': 1}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 8.435331 per second
INFO:root:Total runtime in seconds: 0.118549
INFO:root:Total runtime in minutes: 0.001976
```

### Validate the topic for NewOrderSingle was created

```
gcloud pubsub topics list --format json | jq .[].name | grep NewOrderSingle |tr -d \" | cut -f4 -d/
```

## Create a topic from a schema, subscribe to the topic, publish JSON-encoded Avro to the topic

### Manufacture topic

```
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/testfiles/42_order_single.txt'
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX42.xml'

bin/txcode \
  --create_schemas_only \
  --schema_file FIX42.xml \
  --factory fix \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_encoding json \
  --output_type pubsub \
  --message_type_inclusions NewOrderSingle
```

### Subscribe to topic

```
npm install -g pulltop
pulltop NewOrderSingle
```

### Transcode and publish
```
bin/txcode \
  --source_file 42_order_single.txt \
  --schema_file FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_type pubsub \
  --output_encoding json \
  --lazy_create_resources \
  --message_type_inclusions NewOrderSingle
```

Upon the transcoder's publishing of the message, the payload contents are displayed in the console running `pulltop`.

## Ingest the contents of a Parameta SURFIX file into BigQuery

This example extracts and transcodes messages embedded within a Parameta SURFIX file. A Parameta SURFIX schema is used to decode the messages. Console output of decoded messages is suppressed. The schema and output files are written to BigQuery. Please note that usage of Parameta SURFIX data is subject to your licensing agreement with Parameta.


### CLI

```
bin/txcode  \
    --factory fix \
    --schema_file FIX50SP2.SURFIX.xml \
    --source_file spot_fx.dat \
    --source_file_format_type line_delimited \
    --continue_on_error \
    --quiet \
    --destination_project_id $(gcloud config get-value project) \
    --destination_dataset_id icap \
    --output_type bigquery \
    --message_type_inclusions MarketDataIncrementalRefresh \
    --fix_header_tags 8,9,35,1128,49,56,34,52,10 \
    --lazy_create_resources
```

### Query BigQuery table for midpoint OHLC prices by currency pair for given time window

```
pushd examples/
./get_midpoint_per_pair_window.sh
popd
```

### Output

```
+---------+-----------+-----------+-----------+-----------+
| Symbol  |   open    |   high    |    low    |   close   |
+---------+-----------+-----------+-----------+-----------+
| AED/JPY |   39.4321 |   39.4345 |   39.3855 |   39.4114 |
| ARS/BRL |    0.0346 |    0.0347 |    0.0345 |    0.0346 |
| ARS/JPY |    0.9773 |    0.9774 |    0.9738 |    0.9744 |
| AUD/BRL |    3.3071 |    3.3353 |     3.305 |    3.3302 |
| AUD/CAD |    0.8824 |    0.8845 |    0.8821 |    0.8824 |
| AUD/CHF |    0.6383 |    0.6394 |    0.6377 |    0.6384 |
| AUD/CNH |    4.5667 |    4.5775 |    4.5641 |    4.5725 |
| AUD/CNY |    4.5991 |     4.614 |    4.5941 |    4.6062 |
| AUD/DKK |    4.8471 |    4.8604 |    4.8447 |    4.8542 |
| AUD/EUR |    0.6517 |    0.6535 |    0.6514 |    0.6525 |
| AUD/GBP |    0.5688 |    0.5704 |    0.5683 |    0.5699 |
| AUD/HKD |    5.0719 |      5.09 |     5.068 |    5.0813 |
| AUD/IDR |   9850.21 |    9886.8 |   9844.11 |   9865.46 |
| AUD/INR |   52.6799 |   52.8044 |   52.6175 |   52.6713 |
| AUD/JPY |     93.59 |     93.82 |     93.48 |     93.68 |
| AUD/KRW |    922.13 |    924.15 |    920.98 |     922.1 |
| AUD/MXN |   12.9113 |   12.9555 |   12.9037 |   12.9185 |
| AUD/MYR |    3.0017 |    3.0115 |    2.9985 |    3.0059 |
| AUD/NOK |    6.8366 |    6.8579 |     6.836 |     6.851 |
| AUD/NZD |    1.1359 |    1.1362 |    1.1346 |    1.1346 |
| AUD/RUB |   38.1479 |   38.5303 |   38.1065 |    38.201 |
| AUD/SEK |    7.0513 |    7.0735 |    7.0482 |    7.0652 |
| AUD/SGD |    0.9237 |    0.9264 |    0.9232 |    0.9248 |
| AUD/THB |     24.26 |     24.34 |     24.25 |     24.29 |
| AUD/TRY |   11.9901 |   12.0218 |   11.9763 |   12.0018 |
| AUD/TWD |   20.4805 |   20.5449 |   20.4495 |   20.5173 |
| AUD/USD |    0.6463 |    0.6484 |    0.6456 |     0.647 |
| AUD/ZAR |   11.4539 |   11.4785 |   11.4476 |   11.4671 |
| BOB/BRL |      0.74 |    0.7441 |    0.7397 |    0.7422 |
| BRL/ARS |   28.9562 |   28.9783 |    28.825 |   28.8742 |
| BRL/AUD |    0.3024 |    0.3026 |    0.2998 |    0.3003 |
| BRL/BOB |    1.3516 |    1.3522 |    1.3441 |    1.3478 |
| BRL/CHF |     0.193 |    0.1933 |    0.1914 |    0.1916 |
| BRL/CLP |  181.1094 |  181.2199 |  179.9797 |  181.0353 |
| BRL/CNY |    1.3907 |    1.3915 |    1.3817 |    1.3827 |
| BRL/COP |    885.16 |    885.81 |    868.77 |    869.53 |
| BRL/EUR |    0.1971 |    0.1972 |    0.1958 |     0.196 |
| BRL/GBP |     0.172 |    0.1721 |    0.1708 |    0.1712 |
| BRL/HKD |    1.5344 |     1.535 |    1.5242 |    1.5249 |
| BRL/IDR |   2979.41 |   2981.62 |    2960.6 |   2962.91 |
| BRL/ILS |    0.6885 |    0.6886 |     0.684 |     0.685 |
| BRL/JPY |     28.31 |    28.313 |    28.104 |    28.134 |
| BRL/MXN |     3.906 |    3.9085 |    3.8765 |    3.8787 |
| BRL/NZD |    0.3435 |    0.3437 |    0.3404 |    0.3408 |
| BRL/PEN |    0.7748 |    0.7752 |    0.7697 |    0.7704 |
| BRL/RUB |    11.533 |   11.5762 |   11.4605 |   11.4716 |
| BRL/VES |    1.5981 |     1.599 |    1.5877 |     1.589 |
| CAD/AUD |    1.1328 |    1.1338 |    1.1304 |    1.1329 |
| CAD/CHF |    0.7232 |     0.724 |    0.7221 |    0.7234 |
| CAD/CNY |    5.2107 |    5.2199 |    5.2073 |    5.2191 |
| CAD/DKK |     5.492 |    5.5018 |    5.4846 |    5.4992 |
| CAD/GBP |    0.6444 |    0.6461 |    0.6436 |     0.646 |
| CAD/IDR |  11164.56 |  11185.04 |  11158.03 |  11181.75 |
| CAD/JPY |    106.05 |    106.19 |    105.95 |    106.16 |
| CAD/MXN |    14.633 |     14.65 |    14.624 |    14.641 |
| CAD/NGN |    318.85 |    319.39 |    317.03 |    317.08 |
| CAD/NOK |    7.7488 |    7.7702 |    7.7321 |    7.7627 |
| CAD/RUB |   43.2212 |   43.5796 |   43.1926 |   43.2814 |
| CAD/SEK |    7.9894 |    8.0081 |     7.981 |     8.004 |
| CAD/SGD |    1.0468 |     1.048 |    1.0462 |    1.0479 |
| CAD/THB |      27.5 |     27.56 |     27.47 |     27.53 |
| CAD/TRY |   13.5822 |   13.6034 |   13.5696 |   13.5973 |
| CAD/USD |    0.7323 |    0.7336 |    0.7318 |    0.7334 |
| CAD/ZAR |   12.9811 |   12.9975 |   12.9564 |   12.9911 |
| CHF/CNH |    7.1549 |    7.1646 |    7.1494 |    7.1605 |
| CHF/CZK |     25.06 |    25.122 |    25.043 |    25.098 |
| CHF/DKK |    7.5928 |    7.6106 |    7.5878 |    7.6038 |
| CHF/HKD |     7.947 |    7.9672 |    7.9406 |    7.9599 |
| CHF/HUF |    426.82 |    428.44 |    426.43 |    427.62 |
| CHF/IDR |  15434.76 |  15475.49 |  15423.83 |  15458.23 |
| CHF/INR |   82.5162 |   82.6534 |   82.4347 |   82.5433 |
| CHF/JPY |     146.6 |    146.88 |    146.45 |    146.77 |
| CHF/MXN |   20.2299 |   20.2781 |   20.2147 |   20.2413 |
| CHF/NGN |   440.879 |   441.997 |   438.297 |    438.43 |
| CHF/NOK |   10.7134 |   10.7447 |   10.6985 |   10.7327 |
| CHF/PLN |     4.917 |     4.925 |     4.912 |     4.919 |
| CHF/RUB |    59.754 |    60.299 |   59.7056 |   59.8327 |
| CHF/SGD |    1.4472 |    1.4501 |    1.4463 |    1.4488 |
| CHF/TRY |   18.7821 |     18.82 |   18.7585 |   18.8008 |
| CHF/ZAR |   17.9444 |   17.9752 |   17.9253 |   17.9641 |
| CLP/BRL |  0.005521 |  0.005556 |  0.005518 |  0.005523 |
| CLP/JPY |    0.1563 |    0.1566 |    0.1554 |    0.1554 |
| CNH/AUD |    0.2189 |    0.2191 |    0.2185 |    0.2188 |
| CNH/HKD |    1.1106 |    1.1123 |    1.1102 |    1.1113 |
| CNH/KRW |    201.91 |    201.95 |    201.68 |    201.69 |
| CNH/THB |    5.3144 |    5.3208 |    5.3084 |    5.3149 |
| CNY/AUD |    0.2175 |    0.2177 |    0.2167 |    0.2172 |
| CNY/BRL |    0.7192 |    0.7238 |    0.7187 |    0.7232 |
| CNY/CAD |    0.1919 |     0.192 |    0.1916 |    0.1916 |
| CNY/HKD |    1.1031 |    1.1032 |    1.1031 |    1.1032 |
| CNY/IDR | 2142.7766 | 2142.7766 | 2142.7766 | 2142.7766 |
| CNY/INR |   11.4556 |   11.4571 |   11.4333 |   11.4385 |
| CNY/JPY |   20.3531 |   20.3541 |   20.3295 |   20.3412 |
| CNY/KRW |    200.53 |    200.54 |    200.07 |     200.2 |
| CNY/MYR |    0.6527 |    0.6527 |    0.6527 |    0.6527 |
| CNY/PHP |    8.2569 |    8.2584 |    8.2532 |    8.2552 |
| CNY/SGD |     0.201 |     0.201 |    0.2007 |    0.2007 |
| CNY/THB |    5.2768 |    5.2797 |    5.2712 |    5.2754 |
| CNY/TWD |    4.4535 |    4.4544 |    4.4507 |    4.4534 |
| COP/BRL |   0.11294 |    0.1151 |   0.11289 |   0.11504 |
+---------+-----------+-----------+-----------+-----------+   
```

