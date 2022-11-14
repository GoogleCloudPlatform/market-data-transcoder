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

import io
import json
import logging
import os
from concurrent import futures
from typing import Callable

import avro
import fastavro
from avro.io import DatumWriter, BinaryEncoder
from fastavro import parse_schema
from google.api_core.exceptions import AlreadyExists, NotFound, InvalidArgument
from google.cloud import pubsub_v1
from google.cloud.pubsub import SchemaServiceClient
from google.cloud.pubsub_v1.publisher.futures import Future
from google.pubsub_v1 import Encoding, Topic, UpdateTopicRequest
from google.pubsub_v1.types import Schema

from transcoder.message import DatacastField, DatacastSchema
from transcoder.output import OutputManager
from transcoder.output.exception import OutputNotAvailableError, PubSubTopicSchemaOutOfSyncError
from transcoder.output.google_cloud.Constants import GOOGLE_PACKAGED_SOLUTION_LABEL_DICT, GOOGLE_PACKAGED_SOLUTION_KEY, \
    GOOGLE_PACKAGED_SOLUTION_VALUE


class PubSubOutputManager(OutputManager):
    """Manages creation of Pub/Sub topic and schema objects"""

    @staticmethod
    def output_type_identifier():
        return 'pubsub'

    def __init__(self, project_id: str, output_encoding: str, output_prefix: str = None,
                 lazy_create_resources: bool = False, create_schema_enforcing_topics: bool = True):
        super().__init__(lazy_create_resources=lazy_create_resources)
        self.project_id = project_id
        self.is_binary_encoded = output_encoding.lower() == "binary"
        self.use_fast_avro = True
        self.output_prefix = output_prefix
        self.create_schema_enforcing_topics = create_schema_enforcing_topics
        self.project_path = f"projects/{project_id}"

        self.publisher = pubsub_v1.PublisherClient()
        self.topics = list(self.publisher.list_topics(request={"project": self.project_path}))
        for topic in self.topics:
            topic_id = os.path.basename(topic.name)
            self.existing_schemas.update({topic_id: True})

        self.schema_client = SchemaServiceClient()
        self.schemas = list(self.schema_client.list_schemas(request={"parent": self.project_path}))

        self.avro_schemas = {}

        if self.lazy_create_resources is True:
            for schema in self.schemas:
                schema_id = os.path.basename(schema.name)
                self.avro_schemas[schema_id] = json.loads(self._get_schema_avro(schema.name).definition)

        self.publish_futures = []
        self.publish_futures_data = {}

    def _does_topic_schema_exist(self, schema_id):
        return self._get_schema(schema_id) is not None

    def _does_topic_exist(self, topic_path):
        return self._get_topic(topic_path) is not None

    def _get_schema(self, schema_path):
        result = list(filter(lambda x: x.name == schema_path, self.schemas))
        if len(result) == 1:
            return result
        return None

    def _get_schema_avro(self, schema_path):
        result = self.schema_client.get_schema(request={"name": schema_path})
        return result

    def _get_topic(self, topic_path):
        result = list(filter(lambda x: x.name == topic_path, self.topics))
        if len(result) == 1:
            return result[0]
        return None

    def _create_field(self, field: DatacastField):
        return field.create_avro_field()

    def _add_schema(self, schema: DatacastSchema):  # pylint: disable=too-many-locals
        schema_id = schema.name
        topic_id = schema.name

        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        schema_path = self.schema_client.schema_path(self.project_id, schema.name)

        _fields = self._get_field_list(schema.fields)
        avsc_schema = {'type': 'record', 'namespace': 'sbeMessage', 'name': schema.name, 'fields': _fields}
        self.avro_schemas[schema.name] = avsc_schema

        if self.create_schema_enforcing_topics is True:
            jsoned_avsc_schema = json.dumps(avsc_schema)
            create_schema = False

            if self._does_topic_schema_exist(schema_path) is True:
                result = self._get_schema_avro(schema_path)
                if jsoned_avsc_schema != result.definition:
                    raise PubSubTopicSchemaOutOfSyncError(
                        f'The schema "{schema_path}" differs from the definition for schema "{schema.name}"\nGenerated: {jsoned_avsc_schema}\nExisting: {result.definition}')
            else:
                create_schema = True

            if create_schema is True:
                topic_schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=jsoned_avsc_schema)
                try:
                    result = self.schema_client.create_schema(
                        request={"parent": self.project_path, "schema": topic_schema, "schema_id": schema_id}
                    )
                    logging.debug('Created a schema using an Avro schema:\n%s', result)
                except AlreadyExists:
                    logging.debug('Schema %s already exists.', schema_id)

        _existing_topic = self._get_topic(topic_path)
        if _existing_topic is not None:
            self._check_existing_label(_existing_topic)

            expected_encoding = Encoding.BINARY if self.is_binary_encoded is True else Encoding.JSON
            schema_settings = _existing_topic.schema_settings

            if expected_encoding != schema_settings.encoding:
                raise PubSubTopicSchemaOutOfSyncError(f'The topic "{_existing_topic.name}" has an encoding that '
                                                      f'differs from the '
                                                      f'runtime setting of {str(expected_encoding)}')

            if schema_path != schema_settings.schema:
                raise PubSubTopicSchemaOutOfSyncError(f'The topic "{_existing_topic.name}" has an unexpected schema '
                                                      f'path of "{schema_settings.schema}"')
        else:
            request_dict = {
                "name": topic_path,
                "labels": GOOGLE_PACKAGED_SOLUTION_LABEL_DICT
            }

            if self.create_schema_enforcing_topics is True:
                request_dict["schema_settings"] = {
                    "schema": schema_path,
                    "encoding": Encoding.BINARY if self.is_binary_encoded is True else Encoding.JSON}

            try:
                response = self.publisher.create_topic(request=request_dict)
                logging.debug('Created a topic:\n%s', response)
            except AlreadyExists:
                logging.debug('Topic %s already exists.', topic_id)
            except InvalidArgument as ex:
                logging.error(ex)
                raise

    def _check_existing_label(self, _existing_topic):
        if GOOGLE_PACKAGED_SOLUTION_KEY not in _existing_topic.labels \
                or _existing_topic.labels.get(GOOGLE_PACKAGED_SOLUTION_KEY, None) != GOOGLE_PACKAGED_SOLUTION_VALUE:
            topic = Topic()
            topic.name = _existing_topic.name
            topic.labels[  # pylint: disable=unsupported-assignment-operation
                GOOGLE_PACKAGED_SOLUTION_KEY] = GOOGLE_PACKAGED_SOLUTION_VALUE
            request = UpdateTopicRequest(
                topic=topic,
                update_mask={"paths": ["labels"]}
            )
            try:
                self.publisher.update_topic(request=request)
            except Exception as err:  # pylint: disable=broad-except
                logging.warning("Failed to update topic labels: %s", err)

    @staticmethod
    def get_callback(publish_future: Future, data: str) -> Callable[[pubsub_v1.publisher.futures.Future], None]:  # pylint: disable=unused-argument
        """PubSub future callback function used to log publishing errors"""

        def callback(_publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                logging.debug(_publish_future.result())
            except InvalidArgument as error:
                logging.error(error)
            except futures.TimeoutError:
                logging.error('Publishing %s timed out.', data)

        return callback

    def _write_record(self, record_type_name, record):
        topic_id = record_type_name
        topic_path = self.publisher.topic_path(self.project_id, topic_id)

        if self.is_binary_encoded is True:
            schema = self.avro_schemas[record_type_name]
            bout = io.BytesIO()

            if self.use_fast_avro is True:
                avro_schema = parse_schema(schema)
                if self.create_schema_enforcing_topics is True:
                    # Use schemaless writer
                    fastavro.schemaless_writer(bout, avro_schema, record)
                else:
                    # Use binary writer
                    fastavro.writer(bout, avro_schema, [record], validator=True)
            else:
                jsoned_avsc_schema = json.dumps(schema)
                avro_schema = avro.schema.parse(jsoned_avsc_schema)
                writer = DatumWriter(avro_schema)
                encoder = BinaryEncoder(bout)
                writer.write(record, encoder)

            data = bout.getvalue()
            publish_future = self.publisher.publish(topic_path, data)
            publish_future.add_done_callback(self.get_callback(publish_future, data))
            self.publish_futures.append(publish_future)
        else:
            data: str
            if self.use_fast_avro is True:
                schema = self.avro_schemas[record_type_name]
                sout = io.StringIO()
                avro_schema = parse_schema(schema)
                fastavro.json_writer(sout, avro_schema, [record], write_union_type=self.create_schema_enforcing_topics)
                data = sout.getvalue()
            else:
                if self.create_schema_enforcing_topics is True:
                    raise OutputNotAvailableError('--create_schema_enforcing_topics can not be used with avro.io '
                                                  'library')
                data = json.dumps(record)

            publish_future: Future = self.publisher.publish(topic_path, data.encode("utf-8"))
            publish_future.add_done_callback(self.get_callback(publish_future, data))
            self.publish_futures.append(publish_future)

    def wait_for_completion(self):
        super().wait_for_completion()
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

    def _delete_topic_and_schema(self, topic_path, schema_path):
        self.__delete_topic(topic_path)
        self.__delete_schema(schema_path)

    def __delete_schema(self, schema_path):
        try:
            self.schema_client.delete_schema(request={"name": schema_path})
            logging.debug('Deleted a schema:\n%s', schema_path)
            return True
        except NotFound:
            logging.debug('%s not found.', schema_path)
            return False

    def __delete_topic(self, topic_path):
        try:
            self.publisher.delete_topic(request={"topic": topic_path})
            return True
        except NotFound:
            logging.debug('%s not found.', topic_path)
            return False
