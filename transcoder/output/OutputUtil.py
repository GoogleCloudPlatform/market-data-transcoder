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

from transcoder.output import OutputManager
from transcoder.output.avro import AvroOutputManager
from transcoder.output.avro.FastAvroOutputManager import FastAvroOutputManager
from transcoder.output.diag import DiagnosticOutputManager
from transcoder.output.google_cloud import PubSubOutputManager, BigQueryOutputManager
from transcoder.output.google_cloud.terraform import BigQueryTerraformOutputManager, PubSubTerraformOutputManager
from transcoder.output.json import JsonOutputManager



def all_output_identifiers():
    """List of all available source identifiers"""
    return [
        DiagnosticOutputManager.output_type_identifier(),
        AvroOutputManager.output_type_identifier(),
        FastAvroOutputManager.output_type_identifier(),
        BigQueryOutputManager.output_type_identifier(),
        PubSubOutputManager.output_type_identifier(),
        BigQueryTerraformOutputManager.output_type_identifier(),
        PubSubTerraformOutputManager.output_type_identifier(),
        JsonOutputManager.output_type_identifier()
    ]


def get_output_manager(output_name: str, output_prefix: str = None, output_file_path: str = None,
                       output_encoding: str = None,
                       destination_project_id: str = None,
                       destination_dataset_id: str = None,
                       lazy_create_resources: bool = False,
                       create_schema_enforcing_topics: bool = True):
    """Returns OutputManager instance based on the supplied name"""
    output: OutputManager = None
    if output_name == AvroOutputManager.output_type_identifier():
        output = AvroOutputManager(output_prefix, output_file_path, lazy_create_resources=lazy_create_resources)
    elif output_name == FastAvroOutputManager.output_type_identifier():
        output = FastAvroOutputManager(output_prefix, output_file_path, lazy_create_resources=lazy_create_resources)
    elif output_name == PubSubOutputManager.output_type_identifier():
        output = PubSubOutputManager(destination_project_id, output_encoding=output_encoding,
                                     output_prefix=output_prefix, lazy_create_resources=lazy_create_resources,
                                     create_schema_enforcing_topics=create_schema_enforcing_topics)
    elif output_name == BigQueryOutputManager.output_type_identifier():
        output = BigQueryOutputManager(destination_project_id, destination_dataset_id, output_prefix,
                                       lazy_create_resources=lazy_create_resources)
    elif output_name == BigQueryTerraformOutputManager.output_type_identifier():
        output = BigQueryTerraformOutputManager(destination_project_id, destination_dataset_id, output_file_path)
    elif output_name == PubSubTerraformOutputManager.output_type_identifier():
        output = PubSubTerraformOutputManager(destination_project_id, output_encoding=output_encoding,
                                              create_schema_enforcing_topics=create_schema_enforcing_topics,
                                              output_path=output_file_path)
    elif output_name == DiagnosticOutputManager.output_type_identifier():
        output = DiagnosticOutputManager()
    elif output_name == JsonOutputManager.output_type_identifier():
        output = JsonOutputManager(output_prefix, output_file_path, lazy_create_resources=lazy_create_resources)
    else:
        raise UnsupportedOutputTypeError(f'Output {output_name} is not supported')
    return output


class UnsupportedOutputTypeError(Exception):
    """Exception that is raised when an output name cannot resolve to a child OutputManager class"""
