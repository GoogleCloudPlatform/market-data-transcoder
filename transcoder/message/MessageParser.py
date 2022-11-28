#
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

# pylint: disable=broad-except

import importlib
import logging
import os
from datetime import datetime

from transcoder import LineEncoding
from transcoder.message import DatacastParser, ParsedMessage
from transcoder.message.ErrorWriter import ErrorWriter, TranscodeStep
from transcoder.message.MessageUtil import get_message_parser
from transcoder.output import OutputManager
from transcoder.output.OutputUtil import get_output_manager
from transcoder.source import Source
from transcoder.source.SourceUtil import get_message_source


class MessageParser:  # pylint: disable=too-many-instance-attributes
    """Main entry point for message transcoding"""

    def __init__(self,  # pylint: disable=too-many-arguments),too-many-locals
                 factory, schema_file_path: str,
                 source_file_path: str, source_file_encoding: str, source_file_format_type: str,
                 source_file_endian: str, skip_lines: int = 0, skip_bytes: int = 0, message_skip_bytes: int = 0,
                 line_encoding: LineEncoding = None, output_type: str = None, output_path: str = None,
                 output_encoding: str = None, destination_project_id: str = None, destination_dataset_id: str = None,
                 message_handlers: str = None, lazy_create_resources: bool = False,
                 stats_only: bool = False, create_schemas_only: bool = False,
                 continue_on_error: bool = False, error_output_path: str = None, quiet: bool = False,
                 create_schema_enforcing_topics: bool = True, sampling_count: int = None,
                 message_type_inclusions: str = None, message_type_exclusions: str = None,
                 fix_header_tags: str = None, fix_separator: int = 1):
        self.source_file_path = source_file_path
        self.source_file_encoding = source_file_encoding
        self.source_file_format_type = source_file_format_type
        self.source_file_endian = source_file_endian
        self.skip_lines = skip_lines
        self.skip_bytes = skip_bytes
        self.message_skip_bytes = message_skip_bytes
        self.line_encoding = line_encoding
        self.continue_on_error = continue_on_error
        self.error_output_path = error_output_path
        self.lazy_create_resources = lazy_create_resources
        self.quiet = quiet
        self.create_schema_enforcing_topics = create_schema_enforcing_topics
        self.create_schemas_only = create_schemas_only
        self.output_manager: OutputManager = None
        self.message_handlers = {}
        self.all_message_type_handlers = []
        self.all_handlers = []
        self.handlers_enabled = False
        self.file_name_without_extension = os.path.basename(
            os.path.splitext(source_file_path)[0]) if source_file_path else 'stdin'

        self.error_writer = ErrorWriter(prefix=self.file_name_without_extension,
                                        output_path=self.error_output_path)

        if output_type is not None:
            self.output_manager = get_output_manager(output_type,
                                                     output_prefix=self.file_name_without_extension,
                                                     output_file_path=output_path,
                                                     output_encoding=output_encoding,
                                                     destination_project_id=destination_project_id,
                                                     destination_dataset_id=destination_dataset_id,
                                                     lazy_create_resources=lazy_create_resources,
                                                     create_schema_enforcing_topics=create_schema_enforcing_topics)

            if self.output_manager.supports_data_writing() is False:
                self.create_schemas_only = True

        self.setup_handlers(message_handlers)
        self.message_parser: DatacastParser = get_message_parser(factory, schema_file_path,
                                                                 sampling_count=sampling_count,
                                                                 stats_only=stats_only,
                                                                 message_type_inclusions=message_type_inclusions,
                                                                 message_type_exclusions=message_type_exclusions,
                                                                 fix_header_tags=fix_header_tags,
                                                                 fix_separator=fix_separator)

    def setup_handlers(self, message_handlers: str):
        """Initialize MessageHandler instances to employ at runtime"""

        if message_handlers is None or message_handlers == "":
            return
        if self.create_schemas_only is True:
            return

        self.handlers_enabled = True
        handler_strs = message_handlers.split()
        for handler_cls_name in handler_strs:
            module = importlib.import_module('transcoder.message.handler')
            class_ = getattr(module, handler_cls_name)
            instance = class_(self)
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

    def process(self):
        """Entry point for individual message processing"""
        start_time = datetime.now()
        self.process_schemas()

        source: Source = None
        if self.create_schemas_only is False:
            source: Source = get_message_source(self.source_file_path, self.source_file_encoding,
                                                self.source_file_format_type, self.source_file_endian,
                                                skip_lines=self.skip_lines, skip_bytes=self.skip_bytes,
                                                message_skip_bytes=self.message_skip_bytes,
                                                line_encoding=self.line_encoding)

            self.process_data(source)

        if self.output_manager is not None:
            self.output_manager.wait_for_completion()

        if logging.getLogger().isEnabledFor(logging.INFO):
            end_time = datetime.now()
            time_diff = end_time - start_time
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
                logging.info('Source record count: %s', source.record_count)
                logging.info('Processed record count: %s', self.message_parser.record_count)
                logging.info('Processed schema count: %s', self.message_parser.total_schema_count)
                logging.info('Summary of message counts: %s', self.message_parser.record_type_count)
                logging.info('Summary of error message counts: %s', self.message_parser.error_record_type_count)
                logging.info('Message rate: %s per second', round(source.record_count / total_seconds, 6))

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

    def process_data(self, source):
        """Entry point for individual message processing"""
        with source:
            for raw_record in source.get_message_iterator():
                message: ParsedMessage = None
                try:
                    self.error_writer.set_step(TranscodeStep.DECODE_MESSAGE)
                    self.error_writer.set_step(TranscodeStep.PARSE_MESSAGE)
                    message = self.message_parser.process_message(raw_record)

                    if message is None:
                        continue

                    if message.exception is not None:
                        self.handle_exception(raw_record, message, message.exception)

                    # For messages that contain no fields, the dictionary will be empty.
                    # Skip as no schema is created for these.
                    if message.is_empty():
                        continue

                    if self.handlers_enabled is True:
                        self.error_writer.set_step(TranscodeStep.EXECUTE_HANDLERS)
                        for handler in self.all_message_type_handlers + self.message_handlers.get(message.type, []):
                            self.error_writer.set_step(TranscodeStep.EXECUTE_HANDLER, type(handler).__name__)
                            handler.handle(message)

                    if self.output_manager is not None:
                        self.error_writer.set_step(TranscodeStep.WRITE_OUTPUT_RECORD)
                        self.output_manager.write_record(message.name, message.dictionary)

                except Exception as ex:
                    self.handle_exception(raw_record, message, ex)

    def handle_exception(self, raw_record, message, exception):
        """Process exceptions encountered in the message processing runtime"""
        if message is not None:
            self.message_parser.increment_error_summary_count(message.name)
        else:
            self.message_parser.increment_error_summary_count()

        self.error_writer.write_error(raw_record, message, exception)

        if self.continue_on_error is False:
            raise exception
