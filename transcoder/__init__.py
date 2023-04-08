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

import importlib
import logging
import os
import signal
import sys

from datetime import datetime
from enum import Enum


from transcoder.message.MessageUtil import get_message_parser, parse_handler_config
from transcoder.message import DatacastParser
from transcoder.message.factory import all_supported_factory_types
from transcoder.message.ErrorWriter import ErrorWriter, TranscodeStep
from transcoder.output import all_output_identifiers, get_output_manager, OutputManager
from transcoder.source import all_source_identifiers, get_message_source, Source

# pylint: disable=invalid-name
    
class Transcoder:
    """ Main entry point for transcodihg sessions, bounded by a schema, source and parser """


    def __init__(self, factory: str, schema_file_path: str, source_file_path: str, source_file_encoding: str,
                 source_file_format_type: str, source_file_endian: str, prefix_length: int, skip_lines: int,
                 skip_bytes: int,  message_skip_bytes: int, quiet: bool, output_type: str, output_encoding: str,
                 output_path: str, error_output_path: str, destination_project_id: str, destination_dataset_id: str,
                 message_handlers: str, lazy_create_resources: bool, frame_only: bool, stats_only: bool,
                 create_schemas_only: bool, continue_on_error: bool, create_schema_enforcing_topics: bool,
                 sampling_count: int, message_type_inclusions: str, message_type_exclusions: str, fix_header_tags: str,
                 fix_separator: int, base64: bool, base64_urlsafe: bool):

        self.message_handlers = message_handlers
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
        
        if output_type is None:
            output_type = 'length_delimited' if self.frame_only else 'diag'

        self.output_prefix = os.path.basename(
            os.path.splitext(source_file_path)[0]) if source_file_path else 'stdin'

        self.error_writer = ErrorWriter(prefix=self.output_prefix,
                                        output_path=self.error_output_path)

        if create_schemas_only is False:
            self.source = get_message_source(source_file_path, source_file_encoding,
                                            source_file_format_type, source_file_endian,
                                            skip_bytes, skip_lines, message_skip_bytes,
                                            prefix_length, base64, base64_urlsafe)

        self.output_manager = get_output_manager(output_type, self.output_prefix, output_path,
                                                output_encoding, self.prefix_length, destination_project_id,
                                                destination_dataset_id, lazy_create_resources,
                                                create_schema_enforcing_topics)  

        if self.output_manager.supports_data_writing() is False:
            self.create_schemas_only = True

        if self.frame_only is False: # don't need a parser for just framibg
            self.message_parser: DatacastParser = get_message_parser(factory, schema_file_path,
                                                                    sampling_count, frame_only,
                                                                    stats_only, message_type_inclusions,
                                                                    message_type_exclusions, fix_header_tags,
                                                                    fix_separator)

        self.setup_handlers()
               
        
    def transcode(self):
        """Entry point for transcoding session"""
        self.start_time = datetime.now()
        with self.source:
            for raw_msg in self.source.get_message_iterator():
                if self.frame_only:
                    self.output_manager.write_record(None, raw_msg)
                else:
                    if self.output_manager is not None:
                        self.error_writer.set_step(TranscodeStep.PARSE_MESSAGE)
                        msg = self.message_parser.process_message(raw_msg)

                        if msg is None:
                            continue
                    
                    self.error_writer.set_step(TranscodeStep.WRITE_OUTPUT_RECORD)
                    self.output_manager.write_record(msg.name, msg.dictionary)
                    
        if self.output_manager is not None:
            self.output_manager.wait_for_completion()

        self.print_summary()

    def setup_handlers(self):
        """Initialize MessageHandler instances to employ at runtime"""

        if self.message_handlers is None or self.message_handlers == "":
            return
        if self.create_schemas_only is True or self.frame_only is True:
            return

        self.handlers_enabled = True
        handler_strs = self.message_handlers.split(',')
        for handler_spec in handler_strs:
            cls_name = None
            config_dict = None
            if handler_spec.find(':') == -1: # no handler params
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

            if self.message_parser.stats_only is True:
                logging.info('Run in stats_only mode')

            if self.message_parser.use_sampling is True:
                logging.info('Sampling count: %s', self.message_parser.sampling_count)

            if self.message_parser.message_type_inclusions is not None:
                logging.info('Message type inclusions: %s', self.message_parser.message_type_inclusions)
            elif self.message_parser.message_type_exclusions is not None:
                logging.info('Message type exclusions: %s', self.message_parser.message_type_exclusions)

            if self.create_schemas_only is False:
                logging.info('Source record count: %s', self.source.record_count)
                logging.info('Processed record count: %s', self.message_parser.record_count)
                logging.info('Processed schema count: %s', self.message_parser.total_schema_count)
                logging.info('Summary of message counts: %s', self.message_parser.record_type_count)
                logging.info('Summary of error message counts: %s', self.message_parser.error_record_type_count)
                logging.info('Message rate: %s per second', round(self.source.record_count / total_seconds, 6))

            logging.info('Total runtime in seconds: %s', round(total_seconds, 6))
            logging.info('Total runtime in minutes: %s', round(total_seconds / 60, 6))

    def process_schemas(self):
        """Process the schema specified at runtime"""
        spec_schemas = self.message_parser.process_schema()
        for schema in spec_schemas:
            if len(schema.fields) == 0:
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
