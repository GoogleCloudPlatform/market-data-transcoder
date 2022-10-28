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
from transcoder.output.google_cloud import PubSubOutputManager, BigQueryOutputManager


def get_output_manager(output_name: str, output_prefix: str = None, output_file_path: str = None,
                       output_encoding: str = None,
                       destination_project_id: str = None,
                       destination_dataset_id: str = None,
                       lazy_create_resources: bool = False,
                       create_schema_enforcing_topics: bool = True):
    """Returns OutputManager instance based on the supplied name"""
    output: OutputManager = None
    if output_name == 'avro':
        output = AvroOutputManager(output_prefix, output_file_path, lazy_create_resources=lazy_create_resources)
    elif output_name == 'fastavro':
        output = FastAvroOutputManager(output_prefix, output_file_path, lazy_create_resources=lazy_create_resources)
    elif output_name == 'pubsub':
        output = PubSubOutputManager(destination_project_id, output_encoding=output_encoding,
                                     output_prefix=output_prefix, lazy_create_resources=lazy_create_resources,
                                     create_schema_enforcing_topics=create_schema_enforcing_topics)
    elif output_name == 'bigquery':
        output = BigQueryOutputManager(destination_project_id, destination_dataset_id, output_prefix,
                                       lazy_create_resources=lazy_create_resources)
    else:
        raise UnsupportedOutputTypeError(f'Output {output_name} is not supported')
    return output


class UnsupportedOutputTypeError(Exception):
    """Exception that is raised when an output name cannot resolve to a child OutputManager class"""
