#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import csv
import threading
import http.client
import urllib
from datetime import datetime
import collections
import pkg_resources
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta

from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()
truncated = {}

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)
        
def persist_messages(delimiter, quotechar, messages, destination_path, google_folder, config):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    data = {}
    data_lock = threading.Lock()
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=config.get('bucket_name', ''))
    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.info("Unable to parse:\n{}".format(message))
            # raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            filename = o['stream'] + '-' + now + '.csv'
            filename = os.path.expanduser(os.path.join(destination_path, filename))

            flattened_record = flatten(o['record'])

            if o['stream'] not in headers:
                headers[o['stream']] = flattened_record.keys()

            if o['stream'] not in data:
                data[o['stream']] = []

            data[o['stream']].append(flattened_record.values())
            max_rows = 250000 if o['stream'] == 'AD_PERFORMANCE_REPORT' else 1000000
            max_rows = 50000 if o['stream'] == 'proposal' else max_rows
            if len(data[o['stream']]) >= max_rows:
                with data_lock:
                    flush_to_file(o['stream'], data, destination_path, headers, config, storage_client, bq_client)

            state = None
        elif message_type == 'STATE':
            logger.info('Setting state to {}'.format(o['value']))

            if o['value']['stream'] in data and len(data[o['value']['stream']]) > 0:
                with data_lock:
                    flush_to_file(o['value']['stream'], data, destination_path, headers, config, storage_client, bq_client)
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']

            props = o["schema"]["properties"].items()
            date_column = ['DATE(start_time_utc)'] if stream == 'calls_report' else next(filter(lambda x: 'format' in x[1] and x[1]['format'] == 'date', props), None)
            daily = config.get('daily', False) == 'true'

            global truncated
            if daily and date_column is not None and stream not in truncated:
                yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
                bq_client = bigquery.Client(project=config.get('bucket_name', ''))
                bq_dataset = config.get('bq_dataset', '')
                delete_query = f'DELETE FROM `{bq_dataset}`.{stream} WHERE {date_column[0]} = \'{yesterday}\''
                logger.info(f'Truncating table {stream}: {delete_query}')
                delete_job = bq_client.query(delete_query)
                result = delete_job.result()
                logger.info(f'Table truncated: {result}')
                truncated[stream] = True
        else:
            logger.warning("Unknown message type {} in message {}"
                            .format(o['type'], o))

    return state

def flush_to_file(stream, data, destination_path, headers, config, storage_client, bq_client):
    google_folder = config.get('google_folder', '')
    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    filename = stream + '-' + now + '.csv'
    filepath = os.path.expanduser(os.path.join(destination_path, filename))
    logger.info(f'Writing to file {filepath}')

    with open(filepath, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers[stream])
        writer.writerows(data[stream])

    bucket_name = config.get('bucket_name', '')
    daily = config.get('daily', False) == 'true'
    bucket = storage_client.get_bucket(bucket_name)

    if daily:
        yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        google_folder = f'{google_folder}/{yesterday}'

    blob_path = f'{google_folder}/{filename}'
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(filepath)
    logger.info(f'Uploaded to {blob_path}')
    os.remove(filepath)

    if config.get('upload_to_bq', False) == 'true':
        bq_dataset = config.get('bq_dataset', '')
        table_id = f'{bucket_name}.{bq_dataset}.{stream}'
        uri = f'gs://{bucket_name}/{blob_path}'

        job_config = bigquery.LoadJobConfig(
                    autodetect=config.get('autodetect_schema', False),
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    max_bad_records=30)

        logger.info(f'Loading {uri} into {table_id}')
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)

    data[stream] = []

def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-csv',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(config.get('delimiter', ','),
                             config.get('quotechar', '"'),
                             input_messages,
                             config.get('destination_path', ''),
                             config.get('google_folder', ''),
                             config)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
