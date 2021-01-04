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
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = ['https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/bigquery.insertdata']
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer BigQuery Target'

StreamMeta = collections.namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

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

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(schema['properties'][key], key)
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
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            dat = bytes(json.dumps(msg.record) + '\n', 'UTF-8')

            rows[msg.stream].write(dat)
            #rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream 
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            #tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
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

def persist_lines_stream(project_id, dataset_id, lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}
    data_holder = []
    lines_read = False
    stream = None

    if flags.no_records:
        no_records = int(flags.no_records)
    else:
        no_records = 1

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

    for line in lines:
        lines_read = True
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            clean_decimal_start = time.time()
            for key, value in msg.record.items():
                if isinstance(value, decimal.Decimal):
                    msg.record[key] = str(value)
            clean_decimal_end = time.time()
            logger.info("[TIMING] Clean decimal: {} seconds.".format(clean_decimal_end - clean_decimal_start))

            send_to_bq_start = time.time()
            data_holder.append(msg.record)
            if len(data_holder) >= no_records:
            # one Docker container <-> one stream
                logger.info("Sending: {} records".format(len(data_holder)))
                errors[msg.stream] = bigquery_client.insert_rows_json(tables[msg.stream], data_holder)
                rows[msg.stream] += len(data_holder)
                data_holder = []
                send_to_bq_end = time.time()
                logger.info("[TIMING] Send to BigQuery: {} seconds.".format(send_to_bq_end - send_to_bq_start))

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

    # send remaining msg.records from data_holder
    if len(data_holder) > 0 and lines_read and stream:
        logger.info("Sending: {} records".format(len(data_holder)))
        errors[stream] = bigquery_client.insert_rows_json(tables[stream], data_holder)
        rows[stream] += len(data_holder)

    for table in errors.keys():
        if not errors[table]:
            logging.info('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table, tables[table].path))
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
    # Debug only
    # input = ['{"type": "STATE", "value": {"bookmarks": {"BMSCurrencies-ExchangeRates": {"last_replication_method": "LOG_BASED"}}, "currently_syncing": "BMSCurrencies-ExchangeRates"}}\n', '{"type": "SCHEMA", "stream": "ExchangeRates", "schema": {"properties": {"_id": {"type": ["null", "string"]}, "CurrencyFrom": {"type": ["null", "string"]}, "CurrencyTo": {"type": ["null", "string"]}, "CountryCode": {"type": ["null", "string"]}, "Source": {"type": ["null", "integer"]}, "Rate": {"type": ["null", "number"]}, "Multiplier": {"type": ["null", "number"]}, "PublishDate": {"format": "date-time", "type": ["null", "string"]}, "IsCrossRate": {"type": ["null", "boolean"]}, "CreateDate": {"format": "date-time", "type": ["null", "string"]}, "Timestamp": {"format": "date-time", "type": ["null", "string"]}, "AccessId": {"type": ["null", "string"]}}, "type": ["object", "null"], "additionalProperties": false}, "key_properties": ["_id"]}\n', '{"type": "STATE", "value": {"bookmarks": {"BMSCurrencies-ExchangeRates": {"last_replication_method": "LOG_BASED", "oplog_ts_time": 1609683900, "oplog_ts_inc": 10, "version": 1609683902530}}, "currently_syncing": "BMSCurrencies-ExchangeRates"}}\n', '{"type": "ACTIVATE_VERSION", "stream": "ExchangeRates", "version": 1609683902530}\n', '{"type": "SCHEMA", "stream": "ExchangeRates", "schema": {"type": "object", "properties": {"Rate": {"anyOf": [{"type": "number"}, {}]}, "PublishDate": {"anyOf": [{"type": "string", "format": "date-time"}, {}]}, "CreateDate": {"anyOf": [{"type": "string", "format": "date-time"}, {}]}}}, "key_properties": ["_id"]}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fad05a4af9143db1ea9b11b", "CurrencyFrom": "SDG", "CurrencyTo": "BRL", "CountryCode": "BR", "Source": 5, "Rate": 0.53, "Multiplier": 1, "PublishDate": "2016-09-01T00:00:00.000000Z", "IsCrossRate": false, "CreateDate": "2018-04-13T11:42:51.350000Z"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fad05a4af9143db1ea9b11c", "CurrencyFrom": "BHD", "CurrencyTo": "BRL", "CountryCode": "BR", "Source": 5, "Rate": 8.59, "Multiplier": 1, "PublishDate": "2016-09-01T00:00:00.000000Z", "IsCrossRate": false, "CreateDate": "2018-04-13T11:42:51.350000Z"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fad05a4af9143db1ea9b11d", "CurrencyFrom": "DZD", "CurrencyTo": "BRL", "CountryCode": "BR", "Source": 5, "Rate": 0.03, "Multiplier": 1, "PublishDate": "2016-09-01T00:00:00.000000Z", "IsCrossRate": false, "CreateDate": "2018-04-13T11:42:51.350000Z"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fad05a4af9143db1ea9b11e", "CurrencyFrom": "KWD", "CurrencyTo": "BRL", "CountryCode": "BR", "Source": 5, "Rate": 10.73, "Multiplier": 1, "PublishDate": "2016-09-01T00:00:00.000000Z", "IsCrossRate": false, "CreateDate": "2018-04-13T11:42:51.350000Z"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fad05a4af9143db1ea9b11f", "CurrencyFrom": "GMD", "CurrencyTo": "BRL", "CountryCode": "BR", "Source": 5, "Rate": 0.08, "Multiplier": 1, "PublishDate": "2016-09-01T00:00:00.000000Z", "IsCrossRate": false, "CreateDate": "2018-04-13T11:42:51.350000Z"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460593", "CurrencyFrom": "THB", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "5.32532", "Multiplier": 100, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.310000Z", "Timestamp": "2020-12-31T16:00:07.721000Z", "AccessId": "073618"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460594", "CurrencyFrom": "TRY", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "2.14617", "Multiplier": 10, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.397000Z", "Timestamp": "2020-12-31T16:00:07.721000Z", "AccessId": "84ef1d"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460595", "CurrencyFrom": "USD", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "1.59386", "Multiplier": 1, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.480000Z", "Timestamp": "2020-12-31T16:00:07.721000Z", "AccessId": "add306"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460596", "CurrencyFrom": "ZAR", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "1.08525", "Multiplier": 10, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.563000Z", "Timestamp": "2020-12-31T16:00:07.721000Z", "AccessId": "8e2646"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460597", "CurrencyFrom": "XAU", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "3011.82000", "Multiplier": 1, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.647000Z", "Timestamp": "2020-12-31T16:00:07.722000Z", "AccessId": "c309a9"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460598", "CurrencyFrom": "AED", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "0.43543", "Multiplier": 1, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2020-12-31T15:51:15.730000Z", "Timestamp": "2020-12-31T16:00:07.722000Z", "AccessId": "5c55f8"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "RECORD", "stream": "ExchangeRates", "record": {"_id": "5fedf58794ef6e0001460599", "CurrencyFrom": "EUR", "CurrencyTo": "BGN", "CountryCode": "BG", "Source": 3, "Rate": "1.95600", "Multiplier": 1, "IsCrossRate": false, "PublishDate": "2020-12-31T00:00:00.000000Z", "CreateDate": "2015-10-13T00:00:00.000000Z", "Timestamp": "2020-12-31T16:00:07.722000Z", "AccessId": "34ce10"}, "version": 1609683902530, "time_extracted": "2021-01-03T14:25:02.620473Z"}\n', '{"type": "ACTIVATE_VERSION", "stream": "ExchangeRates", "version": 1609683902530}\n', '{"type": "ACTIVATE_VERSION", "stream": "ExchangeRates", "version": 1609683902530}\n', '{"type": "STATE", "value": {"bookmarks": {"BMSCurrencies-ExchangeRates": {"last_replication_method": "LOG_BASED", "oplog_ts_time": 1609684043, "oplog_ts_inc": 7, "version": 1609683902530, "initial_full_table_complete": true}}, "currently_syncing": null}}\n']



    logger.info("Input length: {}".format(len(input)))
    record_cnt = 0
    for line in input:
        try:
            obj = json.loads(line)
            # print(obj.keys())
            if obj['type'] == "RECORD":
                record_cnt += 1
        except:
            pass
    logger.info("Record count: {}".format(record_cnt))

    if config.get('stream_data', True):
        state = persist_lines_stream(config['project_id'], config['dataset_id'], input, validate_records=validate_records)
    else:
        state = persist_lines_job(config['project_id'], config['dataset_id'], input, truncate=truncate, validate_records=validate_records)

    emit_state(state)
    logger.debug("Exiting normally")



if __name__ == '__main__':
    main()
