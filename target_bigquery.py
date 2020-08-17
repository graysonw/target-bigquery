#!/usr/bin/env python3

import argparse
import io
import sys
import json
import string
import simplejson
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources

from jsonschema import validate
import singer

from oauth2client import tools
from tempfile import TemporaryFile

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

# Remove underscores
PUNCTUATION = string.punctuation.replace('_','')

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/bigquery.insertdata']
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer BigQuery Target'

StreamMeta = collections.namedtuple(
    'StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def clear_dict_hook(items):
    return {k: v if v is not None else '' for k, v in items}


def get_type_from_schema_property(types):
    """ Get other type that is not 'null' """
    if isinstance(types, list):
        return [t for t in types if t != "null"][0]
    return types


def define_schema(field, name):
    schema_name = name
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if 'type' not in field and 'anyOf' in field:
        for types in field['anyOf']:
            if types['type'] == 'null':
                schema_mode = 'NULLABLE'
            else:
                field = types

    if isinstance(field['type'], list):
        if "null" in field['type']:
            schema_mode = 'NULLABLE'
        else:
            schema_mode = 'required'

        schema_type = get_type_from_schema_property(field['type'])
    else:
        schema_type = field['type']

    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field))

    if schema_type == "array":
        schema_type_list = field.get('items').get('type')
        schema_type = get_type_from_schema_property(schema_type_list)
        schema_mode = "REPEATED"

        if schema_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field.get('items')))

    if schema_type == "string":
        if "format" in field:
            if field['format'] == "date-time":
                schema_type = "timestamp"

    if schema_type == 'number':
        schema_type = 'FLOAT'

    return (schema_name, schema_type, schema_mode,
            schema_description, schema_fields)


def build_schema(schema):
    SCHEMA = []
    for key in schema['properties'].keys():

        if not (bool(schema['properties'][key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, \
            schema_description, schema_fields = define_schema(
                field=schema['properties'][key],
                name=key)

        SCHEMA.append(SchemaField(schema_name,
                                  schema_type,
                                  schema_mode,
                                  schema_description,
                                  schema_fields))

    return SCHEMA

def enforce_bigquery_column_naming_requirements(column_name,
                                                prefix='col'):

    if not isinstance(column_name, str):
        logger.error('Column is not str: {}'.format(column_name))

    # Remove unicode
    column_name = column_name.encode('ascii', 'ignore').decode()

    # Remove punctuation
    column_name = \
        column_name.translate(str.maketrans('', '', PUNCTUATION))

    # Replace spaces with underscores
    column_name = column_name.replace(' ', '_')

    # Pipedrive specific change
    length_ge_fourty = len(column_name) >= 40
    if length_ge_fourty:
        updated_column_name = prefix + '_' + column_name
        return updated_column_name[:128]

    # BigQuery column naming requirements
    # https://cloud.google.com/bigquery/docs/schemas#column_names
    first_character_is_underscore = column_name[0] == '_'
    first_character_is_letter = column_name[0].isalpha()
    if (first_character_is_letter or first_character_is_underscore):
        return column_name[:128]

    updated_column_name = prefix + '_' + column_name
    return updated_column_name[:128]

def convert_dict_keys_to_bigquery_format(record):
    updated_record = {}
    for column_name, entry in record.items():

        updated_column_name = \
                enforce_bigquery_column_naming_requirements(column_name)

        # Recursive call to handle nested dicts
        if isinstance(entry, dict):
            nested_entry = convert_dict_keys_to_bigquery_format(entry)
            entry = nested_entry

        updated_record[updated_column_name] = entry

    return updated_record


def convert_schema_column_names_to_bigquery_format(schema):
    updated_schema_properties = \
        convert_dict_keys_to_bigquery_format(schema['properties'])
    schema['properties'] = updated_schema_properties

    return schema


def persist_lines_job(project_id,
                      dataset_id,
                      lines=None,
                      truncate=False,
                      validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id)

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                log_message = (
                    'A record for stream {} was encountered '
                    'before a corresponding schema')
                raise Exception(log_message.format(msg.stream))

            schema = schemas[msg.stream]

            msg.record = convert_dict_keys_to_bigquery_format(
                record=msg.record)

            if validate_records:
                validate(msg.record, schema)

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data,
            # with a newline character splitting each row.
            dat = bytes(simplejson.dumps(msg.record) + '\n', 'UTF-8')

            rows[msg.stream].write(dat)

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream

            schema = convert_schema_column_names_to_bigquery_format(
                schema=msg.schema)

            schemas[table] = schema
            key_properties[table] = msg.key_properties
            rows[table] = TemporaryFile(mode='w+b')
            errors[table] = None
        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in rows.keys():
        table_ref = bigquery_client.dataset(dataset_id).table(table)

        SCHEMA = build_schema(schemas[table])
        load_config = LoadJobConfig()

        load_config.schema = SCHEMA
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        if truncate:
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        rows[table].seek(0)
        logger.info("loading {} to Bigquery.\n".format(table))

        load_job = bigquery_client.load_table_from_file(
            rows[table],
            table_ref,
            job_config=load_config)

        logger.info("loading job {}".format(load_job.job_id))

    return state


def persist_lines_stream(project_id, dataset_id, lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id)

    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = Dataset(dataset_ref)
    try:
        dataset = bigquery_client.create_dataset(
            Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                log_message = (
                    'A record for stream {} was encountered '
                    'before a corresponding schema')
                raise Exception(log_message.format(msg.stream))

            schema = schemas[msg.stream]

            msg.record = convert_dict_keys_to_bigquery_format(
                record=msg.record)

            if validate_records:
                validate(msg.record, schema)

            # Isolate message throwing BadRequest
            try:
                errors[msg.stream] = bigquery_client.insert_rows_json(
                    tables[msg.stream],
                    [msg.record])
                rows[msg.stream] += 1
            except exceptions.BadRequest:
                logger.error("BadRequest, msg is {}".format(msg))
                raise Exception("BadRequest, msg is {}".format(msg))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream

            schema = convert_schema_column_names_to_bigquery_format(
                schema=msg.schema)

            schemas[table] = schema
            key_properties[table] = msg.key_properties

            table_schema = build_schema(schemas[table])

            tables[table] = bigquery.Table(dataset.table(
                table),
                schema=table_schema)
            rows[table] = 0
            errors[table] = None
            try:
                tables[table] = bigquery_client.create_table(tables[table])
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in errors.keys():
        if not errors[table]:
            logging.info('Loaded {} row(s) into {}:{}'.format(
                rows[table], dataset_id, table, tables[table].path))
            emit_state(state)
        else:
            logging.error('Errors:', errors[table], sep=" ")

    return state


def collect():
    try:
        version = pkg_resources.get_distribution('target-bigquery').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-bigquery',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    with open(flags.config) as input:
        config = json.load(input)

    if not config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()

    if config.get('replication_method') == 'FULL_TABLE':
        truncate = True
    else:
        truncate = False

    validate_records = config.get('validate_records', True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if config.get('stream_data', True):
        state = persist_lines_stream(
            config['project_id'],
            config['dataset_id'],
            input,
            validate_records=validate_records)
    else:
        state = persist_lines_job(config['project_id'],
                                  config['dataset_id'],
                                  input, truncate=truncate,
                                  validate_records=validate_records)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
