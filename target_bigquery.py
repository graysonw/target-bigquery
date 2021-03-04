#!/usr/bin/env python3

import argparse
import decimal
import io
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources
import time
import pickle
from types import ModuleType, FunctionType
from gc import get_referents

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
    parser.add_argument("--data-location", help="specify data location for a dataset")
    parser.add_argument("--no-records", help="Send a specified number of records to BigQuery")
    parser.add_argument('--pickle-location', help="Pickle's file location", required=True)
    parser.add_argument('--request-size', help="Maximum request size in bytes", required=True)
    parser.add_argument('--ensure-ascii', help="Ensure ASCII characters (true/false).", required=True)
    flags = parser.parse_args()
    flags.ensure_ascii = True if flags.ensure_ascii == 'true' else False
except ImportError:
    flags = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = ['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/bigquery.insertdata']
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer BigQuery Target'
MAX_NO_RECORDS = 10000
MAX_PAYLOAD_SIZE = int(flags.request_size)

StreamMeta = collections.namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

BLACKLIST = type, ModuleType, FunctionType


def getsize(obj):
    """sum size of object & members."""
    if isinstance(obj, BLACKLIST):
        raise TypeError('getsize() does not take argument of type: ' + str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size


def handle_decimal_values(obj):
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            result[key] = handle_decimal_values(value)
    elif isinstance(obj, list):
        result = []
        for elem in obj:
            result.append(handle_decimal_values(elem))
    elif isinstance(obj, decimal.Decimal):
        result = str(obj)
    else:
        result = obj
    return result


def handle_empty_arrays(array_nodes, payload):
    for array_steps in array_nodes:
        last_key = array_steps[-1:][0]
        nested_dict = safeget(payload, *array_steps[:-1])
        try:
            nested_value = nested_dict[last_key]
        except KeyError:
            # there is no such field in the document
            # return payload
            continue

        if not nested_value:
            nested_dict.update({last_key: []})
    return payload


def force_fields_to_string(selected_fields, payload, ensure_ascii):
    for field_name in selected_fields:
        field_obj = payload.get(field_name)
        if field_obj:
            payload[field_name] = json.dumps(field_obj, ensure_ascii=ensure_ascii)
        else:
            payload[field_name] = None
    return payload


def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            logger.warning('safeget: KeyError - {} at {}'.format(keys, key))
            return None
    return dct


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def clear_dict_hook(items):
    return {k: v if v is not None else '' for k, v in items}


def define_schema(field, name):
    schema_name = name
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if 'type' not in field and 'anyOf' in field:
        for types in field['anyOf']:
            if types:
                if types['type'] == 'null':
                    schema_mode = 'NULLABLE'
                else:
                    field = types

    if isinstance(field['type'], list):
        if field['type'][0] == "null":
            schema_mode = 'NULLABLE'
        else:
            schema_mode = 'required'
        schema_type = field['type'][-1]
    else:
        schema_type = field['type']
    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field))
    if schema_type == "array":
        schema_type = field.get('items').get('type')
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

    return (schema_name, schema_type, schema_mode, schema_description, schema_fields)


def build_schema(schema):
    SCHEMA = []
    for key in schema['properties'].keys():

        if not (bool(schema['properties'][key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(
            schema['properties'][key], key)
        SCHEMA.append(SchemaField(schema_name, schema_type, schema_mode, schema_description, schema_fields))

    return SCHEMA


def persist_lines_job(project_id, dataset_id, lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    if flags.data_location:
        bigquery_client = bigquery.Client(project=project_id, location=flags.data_location)
    else:
        bigquery_client = bigquery.Client(project=project_id)

    # try:
    #     dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    # except exceptions.Conflict:
    #     pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            dat = bytes(json.dumps(msg.record) + '\n', 'UTF-8')

            rows[msg.stream].write(dat)
            # rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            # tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
            rows[table] = TemporaryFile(mode='w+b')
            errors[table] = None
            # try:
            #     tables[table] = bigquery_client.create_table(tables[table])
            # except exceptions.Conflict:
            #     pass

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
            rows[table], table_ref, job_config=load_config)
        logger.info("loading job {}".format(load_job.job_id))
        logger.info(load_job.result())

    # for table in errors.keys():
    #     if not errors[table]:
    #         print('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table), tables[table].path)
    #     else:
    #         print('Errors:', errors[table], sep=" ")

    return state


def persist_lines_stream(project_id, dataset_id, ensure_ascii, lines=None, validate_records=True, array_nodes=[], force_to_string_fields=[]):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = collections.defaultdict(list)
    data_holder = []
    lines_read = False
    stream = None

    if flags.no_records:
        no_records = int(flags.no_records)
    else:
        logger.info('Number of records not specified. Setting to maximum: {}'.format(MAX_NO_RECORDS))
        no_records = MAX_NO_RECORDS

    if flags.data_location:
        bigquery_client = bigquery.Client(project=project_id, location=flags.data_location)
    else:
        bigquery_client = bigquery.Client(project=project_id)

    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = Dataset(dataset_ref)
    try:
        dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    payload_size = 0
    for line in lines:
        lines_read = True
        # skip SCHEMA messages (except for the the intial one)
        if '{"anyOf": [{' in line:
            continue
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            modified_record = handle_decimal_values(msg.record)
            modified_record = handle_empty_arrays(array_nodes, modified_record)
            modified_record = force_fields_to_string(force_to_string_fields, modified_record, ensure_ascii)

            item_size = getsize(modified_record)
            if payload_size + item_size >= MAX_PAYLOAD_SIZE:
                logger.info('Near max request size. Sending: {} records, payload size: {}.'.format(len(data_holder), payload_size))
                upload_res = bigquery_client.insert_rows_json(tables[msg.stream], data_holder)
                if upload_res:
                    logger.error('Upload error: {}'.format(upload_res))
                else:
                    rows[msg.stream] += len(data_holder)
                data_holder = []
                payload_size = 0
                data_holder.append(modified_record)
                payload_size += item_size
            else:
                if len(data_holder) >= no_records:
                    logger.info(
                        "Max request size not reached, max #records reached. Sending: {} records, payload size: {} bytes.".format(
                            len(data_holder), item_size + payload_size))
                    upload_res = bigquery_client.insert_rows_json(tables[msg.stream], data_holder)
                    if upload_res:
                        logger.error('Upload error: {}'.format(upload_res))
                    else:
                        rows[msg.stream] += len(data_holder)
                    data_holder = []
                    payload_size = 0
                data_holder.append(modified_record)
                payload_size += item_size

            stream = msg.stream

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
            rows[table] = 0
            try:
                tables[table] = bigquery_client.create_table(tables[table])
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    if len(data_holder) > 0 and lines_read and stream:
        logger.info(
            "Remaining records. Sending: {} records, payload size: {} bytes.".format(len(data_holder), payload_size))
        upload_res = bigquery_client.insert_rows_json(tables[stream], data_holder)
        if upload_res:
            logger.error('Upload error: {}'.format(upload_res))
        else:
            rows[stream] += len(data_holder)

    for table in errors.keys():
        if not errors[table]:
            logging.info('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table, tables[table].path))
            emit_state(state)
        else:
            logging.error('Errors:', errors[table])

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
    # Debug only

    #     logger.info("Input length: {}".format(len(input)))
    #     record_cnt = 0
    #     for line in input:
    #         try:
    #             obj = json.loads(line)
    #             # print(obj.keys())
    #             if obj['type'] == "RECORD":
    #                 record_cnt += 1
    #         except:
    #             pass
    #     logger.info("Record count: {}".format(record_cnt))

    # only persist_lines_stream supports empty array transformation
    with open(flags.pickle_location, 'rb') as ph:
        pickled_data = pickle.load(ph)
        array_nodes = pickled_data['array_nodes']
        force_to_string_fields = pickled_data['force_to_string_fields']

    if config.get('stream_data', True):
        state = persist_lines_stream(project_id=config['project_id'], dataset_id=config['dataset_id'], lines=input,
                                     validate_records=validate_records, array_nodes=array_nodes,
                                     force_to_string_fields=force_to_string_fields, ensure_ascii=flags.ensure_ascii)
    else:
        state = persist_lines_job(config['project_id'], config['dataset_id'], input, truncate=truncate,
                                  validate_records=validate_records)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
