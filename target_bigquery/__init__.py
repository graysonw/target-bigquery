#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources
import decimal

from jsonschema import validate
import singer

from oauth2client import tools
from tempfile import TemporaryFile

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

from target_bigquery.schema import build_schema

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
]
CLIENT_SECRET_FILE = "client_secret.json"
APPLICATION_NAME = "Singer BigQuery Target"

StreamMeta = collections.namedtuple(
    "StreamMeta", ["schema", "key_properties", "bookmark_properties"]
)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def clear_dict_hook(items):
    return {k: v if v is not None else "" for k, v in items}


def persist_lines_job(
    project_id,
    dataset_id,
    lines=None,
    truncate=False,
    validate_records=True,
    table_suffix=None,
):
    state = None
    schemas = {}
    key_properties = {}
    rows = {}
    errors = {}
    table_suffix = table_suffix or ""

    class DecimalEncoder(json.JSONEncoder):
        # pylint: disable=method-hidden
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                return str(o)
            return super(DecimalEncoder, self).default(o)

    bigquery_client = bigquery.Client(project=project_id)

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            table_name = msg.stream + table_suffix

            if table_name not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        table_name
                    )
                )

            schema = schemas[table_name]

            if validate_records:
                validate(msg.record, schema)

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            dat = bytes(json.dumps(msg.record, cls=DecimalEncoder) + "\n", "UTF-8")

            rows[table_name].write(dat)

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table_name = msg.stream + table_suffix

            if table_name in rows:
                continue

            schemas[table_name] = msg.schema
            key_properties[table_name] = msg.key_properties
            rows[table_name] = TemporaryFile(mode="w+b")
            errors[table_name] = None

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

        logger.info("loading {} to Bigquery.\n".format(table))

        try:
            load_job = bigquery_client.load_table_from_file(
                rows[table], table_ref, job_config=load_config, rewind=True
            )
            logger.info("loading job {}".format(load_job.job_id))
            logger.info(load_job.result())
        except exceptions.BadRequest as err:
            logger.error(
                "failed to load table {} from file: {}".format(table, str(err))
            )
            if load_job.errors:
                messages = [
                    f"reason: {err['reason']}, message: {err['message']}"
                    for err in load_job.errors
                ]
                logger.error("errors:\n{}".format("\n".join(messages)))
            raise

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
        dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(
            dataset_ref
        )
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
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        msg.stream
                    )
                )

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            err = None
            try:
                err = bigquery_client.insert_rows_json(tables[msg.stream], [msg.record])
            except Exception as exc:
                logger.error(
                    f"failed to insert rows for {tables[msg.stream]}: {str(exc)}\n{msg.record}"
                )
                raise

            errors[msg.stream] = err
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            tables[table] = bigquery.Table(
                dataset.table(table), schema=build_schema(schemas[table])
            )
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
            logging.info(
                "Loaded {} row(s) from {} into {}:{}".format(
                    rows[table], dataset_id, table, tables[table].path
                )
            )
            emit_state(state)
        else:
            logging.error("Errors: %s", errors[table])

    return state


def collect():
    try:
        version = pkg_resources.get_distribution("target-bigquery").version
        conn = http.client.HTTPConnection("collector.singer.io", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-bigquery",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug("Collection request failed")


def main():
    with open(flags.config) as input:
        config = json.load(input)

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to stitchdata.com. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=collect).start()

    if config.get("replication_method") == "FULL_TABLE":
        truncate = True
    else:
        truncate = False

    table_suffix = config.get("table_suffix")

    validate_records = config.get("validate_records", True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    if config.get("stream_data", True):
        state = persist_lines_stream(
            config["project_id"],
            config["dataset_id"],
            input,
            validate_records=validate_records,
        )
    else:
        state = persist_lines_job(
            config["project_id"],
            config["dataset_id"],
            input,
            truncate=truncate,
            validate_records=validate_records,
            table_suffix=table_suffix,
        )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
