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

import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

from transcoder.message import DatacastField, DatacastSchema
from transcoder.output import OutputManager
from transcoder.output.exception import BigQueryTableSchemaOutOfSyncError
from transcoder.output.google_cloud.Constants import GOOGLE_PACKAGED_SOLUTION_KEY, GOOGLE_PACKAGED_SOLUTION_LABEL_DICT, \
    GOOGLE_PACKAGED_SOLUTION_VALUE


class BigQueryOutputManager(OutputManager):
    """Manages creation of BigQuery dataset and table objects"""

    @staticmethod
    def output_type_identifier():
        return 'bigquery'

    def __init__(self, project_id: str, dataset_id, output_prefix: str = None, lazy_create_resources: bool = False):
        super().__init__(lazy_create_resources=lazy_create_resources)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.output_prefix = output_prefix
        self.client = bigquery.Client(project=project_id)

        if self._does_dataset_exist(self.dataset_ref) is False:
            self._create_dataset(self.dataset_ref)
        else:
            dataset = self.client.get_dataset(self.dataset_ref)
            if GOOGLE_PACKAGED_SOLUTION_KEY not in dataset.labels or \
                    dataset.labels.get(GOOGLE_PACKAGED_SOLUTION_KEY, None) != GOOGLE_PACKAGED_SOLUTION_VALUE:
                dataset.labels.update(GOOGLE_PACKAGED_SOLUTION_LABEL_DICT)
                self.client.update_dataset(dataset, ["labels"])

        self.tables = list(self.client.list_tables(dataset_id))
        for table_id in list(map(lambda x: x.table_id, self.tables)):
            self.existing_schemas.update({table_id: True})

    def _create_field(self, field: DatacastField):
        return field.create_bigquery_field()

    def _does_table_exist(self, name):
        table_ref = bigquery.TableReference(self.dataset_ref, name)
        try:
            self.client.get_table(table_ref)
            logging.debug('Table %s already exists.', table_ref.table_id)
            return True
        except NotFound:
            logging.debug('Table %s is not found.', table_ref.table_id)
            return False

    @staticmethod
    def _is_schema_equal(schema_1, schema_2):
        if len(schema_1) != len(schema_2):
            logging.debug('Schema list length difference')
            return False
        field_count = range(len(schema_1))
        for i in field_count:
            f_1: bigquery.SchemaField = schema_1[i]
            f_2: bigquery.SchemaField = schema_2[i]
            f_1_api_repr = f_1.to_api_repr()
            f_2_api_repr = f_2.to_api_repr()
            if f_1_api_repr != f_2_api_repr:
                logging.debug('Schema field compare is not equal:\nschema_1: %s\nschema_2: %s', f_1_api_repr,
                              f_2_api_repr)
                return False
        return True

    def _add_schema(self, schema: DatacastSchema):
        bq_schema = self._get_field_list(schema.fields)
        table_ref = bigquery.TableReference(self.dataset_ref, schema.name)

        if self._does_table_exist(schema.name) is True:
            existing_table = self.client.get_table(table_ref)

            if GOOGLE_PACKAGED_SOLUTION_KEY not in existing_table.labels \
                    or existing_table.labels.get(GOOGLE_PACKAGED_SOLUTION_KEY, None) != GOOGLE_PACKAGED_SOLUTION_VALUE:
                existing_table.labels.update(GOOGLE_PACKAGED_SOLUTION_LABEL_DICT)
                try:
                    self.client.update_table(existing_table, ["labels"])
                except Exception as err:  # pylint: disable=broad-except
                    logging.warning("Failed to update table labels: %s", err)

            if self._is_schema_equal(existing_table.schema, bq_schema) is False:
                raise BigQueryTableSchemaOutOfSyncError(
                    f'The schema for table "{table_ref}" differs from the definition for schema "{schema.name}"')
        else:
            table = bigquery.Table(table_ref, schema=bq_schema)
            table.labels = GOOGLE_PACKAGED_SOLUTION_LABEL_DICT
            try:
                self.client.create_table(table, exists_ok=True)
            except Conflict as error:
                # MS: 2022-10-12 Adding exists_ok to create_table call so that the Conflict is not raised
                # b/153072942
                # https://cloud.google.com/bigquery/docs/error-messages
                logging.warning('Table conflict, already exists %s: %s', schema.name, error)
            except Exception as error:
                logging.error('Error creating table %s: %s', schema.name, error)
                raise

    def _write_record(self, record_type_name, record):
        table_ref = bigquery.TableReference(self.dataset_ref, record_type_name)
        errors = self.client.insert_rows_json(table_ref, [record])
        if errors:
            logging.error('Encountered errors while inserting rows: %s', errors)

    def _create_dataset(self, dataset_ref):
        dataset = bigquery.Dataset(dataset_ref)
        dataset.labels = GOOGLE_PACKAGED_SOLUTION_LABEL_DICT
        dataset = self.client.create_dataset(dataset, timeout=30)
        logging.debug('Created dataset %s.%s', self.client.project, dataset.dataset_id)

    def _does_dataset_exist(self, dataset_ref) -> bool:
        try:
            self.client.get_dataset(dataset_ref)
            logging.debug('Dataset %s already exists', dataset_ref)
            return True
        except NotFound:
            logging.debug('Dataset %s is not found', dataset_ref)
            return False
