# ```Google Cloud Datacast Solution```
 
##  _Ingest high-performance exchange feeds into Google Cloud_

_This is not an official Google product or service_

### Introduction

The Datacast `transcoder` is a schema-driven, message-oriented utility to simplify the lossless ingestion of common high-performance electronic trading data formats to Google Cloud.

Electronic trading venues such as futures, crypto and securities exchanges have specialized data representation and distribution needs. In particular, efficient message representation is a high priority due to the massive volume of transactions a venue processes. Cloud-native APIs often use JSON for message payloads, but the extra bytes required to represent messages using high-context encodings impose real cost implications in metered computing environments. 

Unlike JSON, YAML, or even CSV, binary-encoded data is low-context and not self-describing -- the instructions for interpreting binary messages must be explicitly provided by producers separately and in advance, and followed by interpreters.

The architecture of the transcoder relies on several principal abstractions, detailed below:

#### Schema

A schema (also known as a data dictionary) is similar to an API specification, but instead of describing API endpoint contracts, it describes the representative format of binary _messages_ that flow between systemsm. The closest comparison might be drawn with table definitions supported by SQL Data Definition Language, but these schemas are used for data in-motion as well as data at-rest.

The transcoder's current input schema support is for Simple Binary Encoding (SBE) XML as well as QuickFIX-styled FIX protocol schema representations (also in XML).

The target schema and message encoding is Avro, which can be encapsulated in either POSIX files or sent over Pub/Sub topics. BigQuery is another output destination option, with each message type in the schema becoming a single table in BigQuery to which messages encountered in the source data will be streamed.

The names of the output resources will individually correspond to the names of the message types defined in the input schema. For example, the transcoder will create and use a Pub/Sub topic named "NewOrderSingle" for publishing FIX `NewOrderSingle` messages found in source data. Similarly, if an output type of `bigquery` is selected, the transcoder will create a `NewOrderSingle` table in the dataset specified by `--destination_dataset_id`. By default, Avro encoded output will be saved to a file named `<message type>.avro` in either the `avroOut` directory or a directory specified using the `--output_path` parameter.

#### Message

A message represents a discrete interaction between two systems sharing a schema. Each message will conform to a single _message type_ as defined in the schema. Specific message types can be included or excluded for processing by passing a comma-delimited string of message type names to the `--message_type_exclusions` and `--message_type_inclusions` parameters.


#### Encoding

Encodings describe how the contents of a message payload are physically represented to systems. Many familiar encodings, such as JSON, YAML or CSV, are self-describing and do not strictly require that applications use a separate schema definition. However, binary encodings such as SBE, Avro and Protocol Buffers require that applications employ the associated schema in order to properly interpret messages.

The transcoder's supported inbound encodings are SBE binary and ASCII-encoded (tag=value) FIX. Outbound encodings for Pub/Sub message payloads can be Avro binary or Avro JSON. In addition, JSON representations of the inbound messages are written to the process's standard output channel as they are processed. This can be disabled with the `--quiet` parameter.

The transcoder supports base64 decoding of messages using the `--base64` option.

#### Transport

A message transport describes the mechanism for transferring messages between systems. This can be data-in-motion, such as an ethernet network, or data-at-rest, such as a file living on a POSIX filesytem or an object residing within cloud storage. Raw message bytes must be unframed from a particular transport, such as length-delimited files or packet capture files. 

The transcoder's currently supported inbound message source transports are PCAP files, length-delimited binary files, and newline-delimited ASCII files. Multicast UDP and Pub/Sub inbound transports are on the roadmap.

Outbound transport options are locally stored Avro files, Pub/Sub topics or BigQuery tables. In parallel, the transcoder process outputs JSON message representations to the system's standard output channel.

#### Message factory

A message factory takes a message payload read from the input source, determines the associated message type from the schema to apply, and performs any adjustments to the message data prior to transcoding. For example, a message producer may use non-standard SBE headers or metadata that you would like to remove or transform. For standard FIX tag/value input sources, the included `fix` message factory may be used.

### CLI usage

```
# List available cli arguments
usage: main.py [-h] --factory {asx,cme,memx,fix} --schema_file SCHEMA_FILE --source_file
               SOURCE_FILE --source_file_format_type
               {pcap,length_delimited,line_delimited,cme_binary_packet} [--base64]
               [--fix_header_tags FIX_HEADER_TAGS] [--message_handlers MESSAGE_HANDLERS]
               [--message_skip_bytes MESSAGE_SKIP_BYTES]
               [--message_type_exclusions MESSAGE_TYPE_EXCLUSIONS | --message_type_inclusions MESSAGE_TYPE_INCLUSIONS]
               [--sampling_count SAMPLING_COUNT] [--skip_bytes SKIP_BYTES]
               [--skip_lines SKIP_LINES] [--source_file_endian {big,little}]
               [--output_path OUTPUT_PATH] [--output_type {avro,fastavro,pubsub,bigquery}]
               [--error_output_path ERROR_OUTPUT_PATH] [--lazy_create_resources]
               [--destination_project_id DESTINATION_PROJECT_ID]
               [--destination_dataset_id DESTINATION_DATASET_ID]
               [--output_encoding {binary,json}]
               [--create_schema_enforcing_topics | --no-create_schema_enforcing_topics]
               [--continue_on_error] [--log {notset,debug,info,warning,error,critical}]
               [-q] [-v]

Datacast Transcoder process input arguments

options:
  -h, --help            show this help message and exit
  --continue_on_error   Indicates if an exception file should be created, and records
                        continued to be processed upon message level exceptions
  --log {notset,debug,info,warning,error,critical}
                        The default logging level
  -q, --quiet           Suppress message output to console
  -v, --version         show program's version number and exit

Input source arguments:
  --factory {asx,cme,memx,fix}
                        Message factory for decoding
  --schema_file SCHEMA_FILE
                        Path to the schema file
  --source_file SOURCE_FILE
                        Path to the source file
  --source_file_format_type {pcap,length_delimited,line_delimited,cme_binary_packet}
                        The source file format
  --base64              Indicates if each individual message extracted from the source is
                        base 64 encoded
  --fix_header_tags FIX_HEADER_TAGS
                        Comma delimited list of fix header tags
  --message_handlers MESSAGE_HANDLERS
                        Comma delimited list of message handlers in priority order
  --message_skip_bytes MESSAGE_SKIP_BYTES
                        Number of bytes to skip before processing individual messages
                        within a repeated length delimited file message source
  --message_type_exclusions MESSAGE_TYPE_EXCLUSIONS
                        Comma-delimited list of message types to exclude when processing
  --message_type_inclusions MESSAGE_TYPE_INCLUSIONS
                        Comma-delimited list of message types to include when processing
  --sampling_count SAMPLING_COUNT
                        To be used for testing only - the sampling count indicates how
                        many of each distinct message type to process, any additional will
                        be skipped
  --skip_bytes SKIP_BYTES
                        Number of bytes to skip before processing the file. Useful for
                        skipping file-level headers
  --skip_lines SKIP_LINES
                        Number of lines to skip before processing the file
  --source_file_endian {big,little}
                        Source file endianness

Output arguments:
  --output_path OUTPUT_PATH
                        Output file path. Defaults to avroOut
  --output_type {avro,fastavro,pubsub,bigquery}
                        Output format type
  --error_output_path ERROR_OUTPUT_PATH
                        Error output file path if --continue_on_error flag enabled.
                        Defaults to errorOut
  --lazy_create_resources
                        Flag indicating that output resources for message types should be
                        only created as messages of each type are encountered in the
                        source data. Default behavior is to create resources for each
                        message type before messages are processed. Particularly useful
                        when working with FIX but only processing a limited set of message
                        types in the source data

Google Cloud arguments:
  --destination_project_id DESTINATION_PROJECT_ID
                        The Google Cloud project ID for the destination resource

BigQuery arguments:
  --destination_dataset_id DESTINATION_DATASET_ID
                        The BigQuery dataset for the destination. If it does not exist, it
                        will be created

Pub/Sub arguments:
  --output_encoding {binary,json}
                        The encoding of the output
  --create_schema_enforcing_topics, --no-create_schema_enforcing_topics
                        Indicates if Pub/Sub schemas should be created and used to
                        validate messages sent to a topic (default: True)
```

# Install requirements
```
pip install -r requirements.txt
```

### Creating a shortcut

If you prefer to make the transcoder available globally on your machine, execute the scripts below. This script will update the permissions on the txcode symlink to grant execute permissions, and add the txcode symlink to your system PATH variable.

Note if using macOS with interactive shells, you will need to change '\~/.bash_profile' to '\~/.bash_rc'.

```
cd datacast
chmod +x bin/txcode
inc_path=$(pwd)/path.bash.inc
echo "
# The next line updates PATH for the Datacast Transcoder.
if [ -f '$inc_path' ]; then . '$inc_path'; fi" \
>> ~/.bashrc
```
