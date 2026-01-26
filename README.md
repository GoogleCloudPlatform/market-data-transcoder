# ```Google Cloud Datacast Solution```
 
##  _Ingest high-performance exchange feeds into Google Cloud_

_This is not an official Google product or service_

### Introduction

The Datacast `transcoder` is a schema-driven, message-oriented utility to simplify the lossless ingestion of common high-performance electronic trading data formats to Google Cloud.

Electronic trading venues have specialized data representation and distribution needs. In particular, efficient message representation is a high priority due to the massive volume of transactions a venue processes. Cloud-native APIs often use JSON for message payloads, but the extra bytes required to represent messages using high-context encodings have cost implications in metered computing environments. 

Unlike JSON, YAML, or even CSV, binary-encoded data is low-context and not self-describing -- the instructions for interpreting binary messages must be explicitly provided by producers separately and in advance, and followed by interpreters.

The architecture of the transcoder relies on several principal abstractions, detailed below:

#### Schema

A schema (also known as a data dictionary) is similar to an API specification, but instead of describing API endpoint contracts, it describes the representative format of binary _messages_ that flow between systems. The closest comparison might be drawn with table definitions supported by SQL Data Definition Language, but these schemas are used for data in-motion as well as data at-rest.

The transcoder's current input schema support is for Simple Binary Encoding (SBE) XML as well as QuickFIX-styled FIX protocol schema representations (also in XML).

Target schema and data elements are rendered based on the specified `output_type`. With no output type specified, the transcoder defaults to displaying the YAML representation of transcoded messages to the console, and does not perform persistent schema transformations. For Avro and JSON, the transcoded schema and data files are encapsulated in POSIX files locally. Direct trancoding to BigQuery and Pub/Sub targets are supported, with the transcoded schemas being applied prior to message ingestion or publishing. Terraform configurations for BigQuery and Pub/Sub resources can also be derived from a specified input schema. The Terraform options only render the configurations locally and do not execute Terraform `apply`. The `--create_schemas_only` option transcodes  schemas in isolation for other output types.

The names of the output resources will individually correspond to the names of the message types defined in the input schema. For example, the transcoder will create and use a Pub/Sub topic named "NewOrderSingle" for publishing FIX `NewOrderSingle` messages found in source data. Similarly, if an output type of `bigquery` is selected, the transcoder will create a `NewOrderSingle` table in the dataset specified by `--destination_dataset_id`. By default, Avro and JSON encoded output will be saved to a file named `<message type>` with the respective extensions in a directory specified using the `--output_path` parameter.

#### Message

A message represents a discrete interaction between two systems sharing a schema. Each message will conform to a single _message type_ as defined in the schema. Specific message types can be included or excluded for processing by passing a comma-delimited string of message type names to the `--message_type_exclusions` and `--message_type_inclusions` parameters.


#### Encoding

Encodings describe how the contents of a message payload are represented to systems. Many familiar encodings, such as JSON, YAML or CSV, are self-describing and do not strictly require that applications use a separate schema definition. However, binary encodings such as SBE, Avro and Protocol Buffers require that applications employ the associated schema in order to properly interpret messages.

The transcoder's supported inbound encodings are SBE binary and ASCII-encoded (tag=value) FIX. Outbound encodings for Pub/Sub message payloads can be Avro binary or Avro JSON. Local files can be generated in either Avro or JSON.

The transcoder supports base64 decoding of messages using the `--base64` and `--base64_urlsafe` options.

#### Transport

A message transport describes the mechanism for transferring messages between systems. This can be data-in-motion, such as an ethernet network, or data-at-rest, such as a file living on a POSIX filesytem or an object residing within cloud storage. Raw message bytes must be unframed from a particular transport, such as length-delimited files or packet capture files. 

The transcoder's currently supported inbound message source transports are PCAP files, length-delimited binary files, and newline-delimited ASCII files. Multicast UDP and Pub/Sub inbound transports are on the roadmap.

Outbound transport options are locally stored Avro and JSON POSIX files, and Pub/Sub topics or BigQuery tables. If no `output_type` is specified, the transcoded messages are output to the console encoded in YAML and not persisted automatically. Additionally, Google Cloud resource definitions for specified schemas can be encapsulated in Terraform configurations.

#### Message factory

A message factory takes a message payload read from the input source, determines the associated message type from the schema to apply, and performs any adjustments to the message data prior to transcoding. For example, a message producer may use non-standard SBE headers or metadata that you would like to remove or transform. For standard FIX tag/value input sources, the included `fix` message factory may be used.

### CLI usage

```
usage: txcode  [-h] [--factory {cme,itch,memx,fix}]
               [--schema_file SCHEMA_FILE] [--source_file SOURCE_FILE]
               [--source_file_encoding SOURCE_FILE_ENCODING]
               --source_file_format_type
               {pcap,length_delimited,line_delimited,cme_binary_packet}
               [--base64 | --base64_urlsafe]
               [--fix_header_tags FIX_HEADER_TAGS]
               [--fix_separator FIX_SEPARATOR]
               [--message_handlers MESSAGE_HANDLERS]
               [--message_skip_bytes MESSAGE_SKIP_BYTES]
               [--prefix_length PREFIX_LENGTH]
               [--message_type_exclusions MESSAGE_TYPE_EXCLUSIONS | --message_type_inclusions MESSAGE_TYPE_INCLUSIONS]
               [--sampling_count SAMPLING_COUNT] [--skip_bytes SKIP_BYTES]
               [--skip_lines SKIP_LINES] [--source_file_endian {big,little}]
               [--output_path OUTPUT_PATH]
               [--output_type {diag,avro,fastavro,bigquery,pubsub,bigquery_terraform,pubsub_terraform,jsonl,length_delimited}]
               [--error_output_path ERROR_OUTPUT_PATH]
               [--lazy_create_resources] [--frame_only] [--stats_only]
               [--create_schemas_only]
               [--destination_project_id DESTINATION_PROJECT_ID]
               [--destination_dataset_id DESTINATION_DATASET_ID]
               [--output_encoding {binary,json}]
               [--create_schema_enforcing_topics | --no-create_schema_enforcing_topics]
               [--continue_on_error]
               [--log {notset,debug,info,warning,error,critical}] [-q] [-v]

Datacast Transcoder process input arguments

options:
  -h, --help            show this help message and exit
  --continue_on_error   Indicates if an exception file should be created, and
                        records continued to be processed upon message level
                        exceptions
  --log {notset,debug,info,warning,error,critical}
                        The default logging level
  -q, --quiet           Suppress message output to console
  -v, --version         show program's version number and exit

Input source arguments:
  --factory {cme,itch,memx,fix}
                        Message factory for decoding
  --schema_file SCHEMA_FILE
                        Path to the schema file
  --source_file SOURCE_FILE
                        Path to the source file
  --source_file_encoding SOURCE_FILE_ENCODING
                        The source file character encoding
  --source_file_format_type {pcap,length_delimited,line_delimited,cme_binary_packet}
                        The source file format
  --base64              Indicates if each individual message extracted from
                        the source is base 64 encoded
  --base64_urlsafe      Indicates if each individual message extracted from
                        the source is base 64 url safe encoded
  --fix_header_tags FIX_HEADER_TAGS
                        Comma delimited list of fix header tags
  --fix_separator FIX_SEPARATOR
                        The unicode int representing the fix message separator
  --message_handlers MESSAGE_HANDLERS
                        Comma delimited list of message handlers in priority
                        order
  --message_skip_bytes MESSAGE_SKIP_BYTES
                        Number of bytes to skip before processing individual
                        messages within a repeated length delimited file
                        message source
  --prefix_length PREFIX_LENGTH
                        How many bytes to use for the length prefix of length-
                        delimited binary sources
  --message_type_exclusions MESSAGE_TYPE_EXCLUSIONS
                        Comma-delimited list of message types to exclude when
                        processing
  --message_type_inclusions MESSAGE_TYPE_INCLUSIONS
                        Comma-delimited list of message types to include when
                        processing
  --sampling_count SAMPLING_COUNT
                        Halt processing after reaching this number of
                        messages. Applied after all Handlers are executed per
                        message
  --skip_bytes SKIP_BYTES
                        Number of bytes to skip before processing the file.
                        Useful for skipping file-level headers
  --skip_lines SKIP_LINES
                        Number of lines to skip before processing the file
  --source_file_endian {big,little}
                        Source file endianness

Output arguments:
  --output_path OUTPUT_PATH
                        Output file path. Defaults to avroOut
  --output_type {diag,avro,fastavro,bigquery,pubsub,bigquery_terraform,pubsub_terraform,jsonl,length_delimited}
                        Output format type
  --error_output_path ERROR_OUTPUT_PATH
                        Error output file path if --continue_on_error flag
                        enabled. Defaults to errorOut
  --lazy_create_resources
                        Flag indicating that output resources for message
                        types should be only created as messages of each type
                        are encountered in the source data. Default behavior
                        is to create resources for each message type before
                        messages are processed. Particularly useful when
                        working with FIX but only processing a limited set of
                        message types in the source data
  --frame_only          Flag indicating that transcoder should only frame
                        messages to an output source
  --stats_only          Flag indicating that transcoder should only report on
                        message type counts without parsing messages further
  --create_schemas_only
                        Flag indicating that transcoder should only create
                        output resource schemas and not output message data

Google Cloud arguments:
  --destination_project_id DESTINATION_PROJECT_ID
                        The Google Cloud project ID for the destination
                        resource

BigQuery arguments:
  --destination_dataset_id DESTINATION_DATASET_ID
                        The BigQuery dataset for the destination. If it does
                        not exist, it will be created

Pub/Sub arguments:
  --output_encoding {binary,json}
                        The encoding of the output
  --create_schema_enforcing_topics, --no-create_schema_enforcing_topics
                        Indicates if Pub/Sub schemas should be created and
                        used to validate messages sent to a topic
```

### Message handlers

`txcode` supports the execution of _message handler_ classes that can
be used to statefully mutate in-flight streams and messages. For example,
`TimestampPullForwardHandler` will look for a `seconds`-styled ITCH
message (that informs the stream of the prevailing epochs second to
apply to subsequent messages), and append the latest value from
that to all subsequent messages (between instances of the `seconds`
message appearing. This helps individual messages be persisted with
absolute timestamps that require less context to interpret
(i.e. outbound messages contain more than just "nanoseconds past
midnight" for a timestamp.

Another handler is `SequencerHandler`, which appends a sequence number
to all outbound messages. This is useful when processing bulk messages
in length-delimited storage formats where the IP packet headers
containing the original sequence numbers have been stripped.	

`FilterHandler` lets you filter output based upon a specific property
of a message. A common use for this is to filter messages pertaining
only to a particular security identifier or symbol.

Here is a combination of transcoding invocations that can
be used to shard a message universe by trading symbol. First, the mnemonic
trading symbol identifier (`stock`) must be used to find it's associated integer
security identifier (`stock_locate`) from the `stock_directory`
message. `stock_locate` is the identifier included in every
relevant message (as opposed to `stock`, which is absent from
certain message types):

```

txcode --source_file 12302019.NASDAQ_ITCH50 --schema_file totalview-itch-50.xml --message_type_inclusions stock_directory --source_file_format_type length_delimited --factory itch --message_handlers FilterHandler:stock=SPY --sampling_count 1

authenticity: P
etp_flag: Y
etp_leverage_factor: null
financial_status_indicator: ' '
inverse_indicator: null
ipo_flag: ' '
issue_classification: Q
issue_subtype: E
luld_reference_price_tier: '1'
market_category: P
round_lot_size: 100
round_lots_only: N
short_sale_threshold_indicator: N
stock: SPY
stock_locate: 7451
timestamp: 11354508113636
tracking_number: 0

INFO:root:Sampled messages: 1
INFO:root:Message type inclusions: ['stock_directory']
INFO:root:Source message count: 7466
INFO:root:Processed message count: 7451
INFO:root:Transcoded message count: 1
INFO:root:Processed schema count: 1
INFO:root:Summary of message counts: {'stock_directory': 7451}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 53260.474108 per second
INFO:root:Total runtime in seconds: 0.140179
INFO:root:Total runtime in minutes: 0.002336
```

Taking the value of the field `stock_locate` from the above message
allows us to filter all messages for that field/value combination. In
addition, we can append a sequence number to all transcoded messages
that are output. The below combination returns the original `stock_directory`
message we used to look up the `stock_locate` code, as well as the
next two messages in the stream that have the same value for `stock_locate`:

```

txcode --source_file 12302019.NASDAQ_ITCH50 --schema_file totalview-itch-50.xml --source_file_format_type length_delimited --factory itch --message_handlers FilterHandler:stock_locate=7451,SequencerHandler --sampling_count 3 

authenticity: P
etp_flag: Y
etp_leverage_factor: null
financial_status_indicator: ' '
inverse_indicator: null
ipo_flag: ' '
issue_classification: Q
issue_subtype: E
luld_reference_price_tier: '1'
market_category: P
round_lot_size: 100
round_lots_only: N
sequence_number: 1
short_sale_threshold_indicator: N
stock: SPY
stock_locate: 7451
timestamp: 11354508113636
tracking_number: 0

reason: ''
reserved: ' '
sequence_number: 2
stock: SPY
stock_locate: 7451
timestamp: 11355134575401
tracking_number: 0
trading_state: T

reg_sho_action: '0'
sequence_number: 3
stock: SPY
stock_locate: 7451
timestamp: 11355134599149
tracking_number: 0

INFO:root:Sampled messages: 3
INFO:root:Source message count: 23781
INFO:root:Processed message count: 23781
INFO:root:Transcoded message count: 3
INFO:root:Processed schema count: 21
INFO:root:Summary of message counts: {'system_event': 1, 'stock_directory': 8906, 'stock_trading_action': 7437, 'reg_sho_restriction': 7437, 'market_participant_position': 0, 'mwcb_decline_level': 0, 'ipo_quoting_period_update': 0, 'luld_auction_collar': 0, 'operational_halt': 0, 'add_order_no_attribution': 0, 'add_order_attribution': 0, 'order_executed': 0, 'order_executed_price': 0, 'order_cancelled': 0, 'order_deleted': 0, 'order_replaced': 0, 'trade': 0, 'cross_trade': 0, 'broken_trade': 0, 'net_order_imbalance': 0, 'retail_price_improvement_indicator': 0}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 80950.257512 per second
INFO:root:Total runtime in seconds: 0.293773
INFO:root:Total runtime in minutes: 0.004896


```

The syntax for handler specifications is:

```
<Handler1>:<Handler1Parameter>=<Handler1Parameter>,<Handler2>
```

Message handlers are deployed in `transcoder/message/handler/`.

# Installation
If you are a user looking to use the CLI or library without making changes, you can install the Market Data Transcoder from [PyPI](https://pypi.org/project/market-data-transcoder) using pip:
```
pip install market-data-transcoder
```

After the pip installation, you can validate that the transcoder is available by the following command:
```
txcode --help
```

# Developers
If you are looking to extend the functionality of the Market Data Transcoder:
```
cd market-data-transcoder
poetry install
```

After installing the required dependencies, you can run the transcoder with the following:
```
txcode
```

# Linting
```bash
poetry run pylint transcoder
```