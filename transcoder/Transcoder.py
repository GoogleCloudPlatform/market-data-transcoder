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

# pylint: disable=broad-except

import importlib
import logging
import os
import signal
import sys
from datetime import datetime

from transcoder.message import DatacastParser, NoParser
from transcoder.message.ErrorWriter import ErrorWriter, TranscodeStep
from transcoder.message.MessageUtil import get_message_parser, parse_handler_config
from transcoder.output import get_output_manager
from transcoder.source import get_message_source


# pylint: disable=invalid-name
class Transcoder:  # pylint: disable=too-many-instance-attributes,too-many-positional-arguments
    """ Main entry point for transcodihg sessions, bounded by a schema, source and parser """

    def __init__(self,  # pylint: disable=too-many-arguments,too-many-locals
                 factory: str, schema_file_path: str, source_file_path: str, source_file_encoding: str,
                 source_file_format_type: str, source_file_endian: str, prefix_length: int, skip_lines: int,
                 skip_bytes: int, message_skip_bytes: int, quiet: bool, output_type: str, output_encoding: str,
                 output_path: str, error_output_path: str, destination_project_id: str, destination_dataset_id: str,
                 message_handlers: str, lazy_create_resources: bool, frame_only: bool, stats_only: bool,
                 create_schemas_only: bool, continue_on_error: bool, create_schema_enforcing_topics: bool,
                 sampling_count: int, message_type_inclusions: str, message_type_exclusions: str, fix_header_tags: str,
                 fix_separator: int, base64: bool, base64_urlsafe: bool):

        signal.signal(signal.SIGINT, self.trap)

        self.message_handler_spec = message_handlers
        self.message_handlers = {}
        self.all_message_type_handlers = []
        self.all_handlers = []
        self.handlers_enabled = False
        self.continue_on_error = continue_on_error
        self.error_output_path = error_output_path
        self.output_encoding = output_encoding
        self.frame_only = frame_only
        self.create_schemas_only = create_schemas_only
        self.lazy_create_resources = lazy_create_resources
        self.prefix_length = prefix_length
        self.quiet = quiet
        self.start_time = None
        self.stats_only = stats_only
        self.sampling_count = sampling_count
        self.transcoded_count = 0

        self.output_prefix = os.path.basename(
            os.path.splitext(source_file_path)[0]) if source_file_path else 'stdin'

        self.error_writer = ErrorWriter(prefix=self.output_prefix,
                                        output_path=self.error_output_path)

        self.output_manager = get_output_manager(output_type, self.output_prefix, output_path,
                                                 output_encoding, self.prefix_length, destination_project_id,
                                                 destination_dataset_id, lazy_create_resources,
                                                 create_schema_enforcing_topics)

        # TODO: think about this abstraction some more
        if self.output_manager.supports_data_writing() is False:
            self.create_schemas_only = True
        else:
            self.source = get_message_source(source_file_path, source_file_encoding,
                                             source_file_format_type, source_file_endian,
                                             skip_bytes, skip_lines, message_skip_bytes,
                                             prefix_length, base64, base64_urlsafe)

        self.message_parser: DatacastParser = NoParser() if self.frame_only else get_message_parser(
            factory,
            schema_file_path,
            stats_only,
            message_type_inclusions,
            message_type_exclusions,
            fix_header_tags,
            fix_separator
        )

        self.setup_handlers()

    def transcode(self):
        """Entry point for transcoding session"""
        self.start_time = datetime.now()
        if self.frame_only is False:
            self.process_schemas()

        with self.source:
            for raw_msg in self.source.get_message_iterator():
                if self.frame_only:  # don't parse message
                    self.message_parser.process_message(raw_msg)
                    self.output_manager.write_record(None, raw_msg)
                else:  # parse message
                    if self.stats_only is False:  # output message
                        self.transcode_message(raw_msg)

                if self.transcoded_count == self.sampling_count:
                    break

        self.print_summary()

    def transcode_message(self, raw):
        """ Transcoding steps executed on each source message """
        self.error_writer.set_step(TranscodeStep.PARSE_MESSAGE)
        msg = None
        try:
            msg = self.message_parser.process_message(raw)

            if msg.exception is not None:
                self.handle_exception(raw, msg, msg.exception)

            if msg.ignored is False:  # passed inclusions / exclusions
                self.execute_handlers(msg)
                if msg.ignored is False:  # passed filters
                    self.error_writer.set_step(TranscodeStep.WRITE_OUTPUT_RECORD)
                    self.output_manager.write_record(msg.name, msg.dictionary)
                    self.transcoded_count += 1

        except Exception as ex:
            self.handle_exception(raw, msg, ex)

    def execute_handlers(self, message):
        """ Executes in sequence the message handlers specified for this transcoding instance """
        if self.handlers_enabled is True:  # execute handlers
            self.error_writer.set_step(TranscodeStep.EXECUTE_HANDLERS)
            for handler in self.all_message_type_handlers + self.message_handlers.get(message.type, []):
                self.error_writer.set_step(TranscodeStep.EXECUTE_HANDLER, type(handler).__name__)
                handler.handle(message)

    def setup_handlers(self):
        """Initialize MessageHandler instances to employ at runtime"""

        if self.message_handler_spec is None or self.message_handler_spec == "":
            return

        self.handlers_enabled = True
        handler_strs = self.message_handler_spec.split(',')
        for handler_spec in handler_strs:
            cls_name = None
            config_dict = None
            if handler_spec.find(':') == -1:  # no handler params
                cls_name = handler_spec
            else:
                cls_name = handler_spec.split(':')[0]
                config_dict = parse_handler_config(handler_spec)

            module = importlib.import_module('transcoder.message.handler')
            class_ = getattr(module, cls_name)
            instance = class_(config_dict)
            self.all_handlers.append(instance)

            if instance.supports_all_message_types is True:
                self.all_message_type_handlers.append(instance)
                continue

            supported_msg_types = instance.supported_message_types
            for supported_type in supported_msg_types:
                if supported_type in self.message_handlers:
                    handler_list = self.message_handlers[supported_type]
                    if instance not in handler_list:
                        self.message_handlers[supported_type].append(instance)
                else:
                    self.message_handlers[supported_type] = [instance]

    def print_summary(self):
        """Print summary of the messages that were processed"""
        if logging.getLogger().isEnabledFor(logging.INFO):
            end_time = datetime.now()
            time_diff = end_time - self.start_time
            total_seconds = time_diff.total_seconds()

            if self.create_schemas_only is True:
                logging.info('Run in create_schemas_only mode')

            if self.output_manager.supports_data_writing() is False:
                logging.info('Output manager \'%s\' does not support message writes',
                             self.output_manager.output_type_identifier())

            if self.frame_only is False:

                if self.message_parser.stats_only is True:
                    logging.info('Run in stats_only mode')

                if self.sampling_count is not None:
                    logging.info('Sampled messages: %s', self.sampling_count)

                if self.message_parser.message_type_inclusions is not None:
                    logging.info('Message type inclusions: %s', self.message_parser.message_type_inclusions)
                elif self.message_parser.message_type_exclusions is not None:
                    logging.info('Message type exclusions: %s', self.message_parser.message_type_exclusions)

                if self.create_schemas_only is False:
                    logging.info('Source message count: %s', self.source.record_count)
                    logging.info('Processed message count: %s', self.message_parser.record_count)
                    logging.info('Transcoded message count: %s', self.transcoded_count)
                    logging.info('Processed schema count: %s', self.message_parser.total_schema_count)
                    logging.info('Summary of message counts: %s', self.message_parser.record_type_count)
                    logging.info('Summary of error message counts: %s', self.message_parser.error_record_type_count)
                    logging.info('Message rate: %s per second', round(self.source.record_count / total_seconds, 6))

            else:
                logging.info('Source record count: %s', self.source.record_count)

            logging.info('Total runtime in seconds: %s', round(total_seconds, 6))
            logging.info('Total runtime in minutes: %s', round(total_seconds / 60, 6))

    def process_schemas(self):
        """Process the schema specified at runtime"""
        spec_schemas = self.message_parser.process_schema()
        for schema in spec_schemas:
            if self.output_manager.supports_zero_field_schemas() is False and len(schema.fields) == 0:
                logging.info('Schema "%s" contains no field definitions, skipping schema creation', schema.name)
                continue

            for handler in self.all_handlers:
                if handler.supports_all_message_types is True \
                        or schema.message_id in handler.supported_message_types:
                    handler.append_manufactured_fields(schema)

            self.output_manager.enqueue_schema(schema)

        # Only need to wait if lazy create is off, and you want to force creation before data is read
        if self.lazy_create_resources is False:
            self.output_manager.wait_for_schema_creation()

    def handle_exception(self, raw_record, message, exception):
        """Process exceptions encountered in the message processing runtime"""
        if message is not None:
            self.message_parser.increment_error_summary_count(message.name)
        else:
            self.message_parser.increment_error_summary_count()

        self.error_writer.write_error(raw_record, message, exception)

        if self.continue_on_error is False:
            raise exception

    def trap(self, _signum, _frame):
        """Trap SIGINT to suppress noisy stack traces and show interim summary"""
        print()
        self.print_summary()
        sys.exit(1)
