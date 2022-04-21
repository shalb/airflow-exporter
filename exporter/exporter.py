#!/usr/bin/env python

import urllib.request
import json
import traceback
import argparse
import sys
import time
import logging
import yaml
import os
import datetime
import prometheus_client
import prometheus_client.core

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--config', default=sys.argv[0] + '.yml', help='config file location')
parser.add_argument('--log_level', help='logging level')
parser.add_argument('--url', help='airflow web UI url')
parser.add_argument('--tasks', help='tasks to execute')
args = parser.parse_args()

# add prometheus decorators
REQUEST_TIME = prometheus_client.Summary('request_processing_seconds', 'Time spent processing request')

def get_config(args):
    '''Parse configuration file and merge with cmd args'''
    for key in vars(args):
        conf[key] = vars(args)[key]
    with open(conf['config']) as conf_file:
        conf_yaml = yaml.load(conf_file, Loader=yaml.FullLoader)
    for key in conf_yaml:
        if not conf.get(key):
            conf[key] = conf_yaml[key]
    tasks = os.environ.get('TASKS')
    if tasks:
        conf['tasks'] = tasks
    conf['tasks'] = conf['tasks'].split(',')
    log_level = os.environ.get('LOG_LEVEL')
    if log_level:
        conf['log_level'] = log_level
    target_url = os.environ.get('TARGET_URL')
    if target_url:
        conf['target_url'] = target_url
    timeout = os.environ.get('TIMEOUT')
    if timeout:
        conf['timeout'] = int(timeout)

def configure_logging():
    '''Configure logging module'''
    log = logging.getLogger(__name__)
    log.setLevel(conf['log_level'])
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=FORMAT)
    return log

# Decorate function with metric.
@REQUEST_TIME.time()
def get_data():
    '''Get data from target service'''
    for task_name in conf['tasks']:
        get_data_function = globals()['get_data_'+ task_name]
        get_data_function()
                
def get_data_health():
    '''Get dags list via API'''
    req = urllib.request.Request('{}/health'.format(conf['target_url']))
    responce = urllib.request.urlopen(req, timeout=conf['timeout'])
    raw_data = responce.read().decode()
    json_data = json.loads(raw_data)
    log.debug('get_data_health: {}'.format(json_data))
    parse_data_health(json_data)

def parse_data_health(json_data):
    '''Parse dags status via API'''
    for service in list(json_data.keys()):
        labels = dict()
        labels['airflow_service'] = label_clean(service)
        labels['target_url'] = label_clean(conf['target_url'])
        metric_name = '{0}_exporter_health_status'.format(conf['name'])
        description = 'Health status: 1 = OK, 0 = Fail.'
        if json_data[service]['status'] == 'healthy':
            value = 1
        else:
            value = 0
        metric = {'metric_name': metric_name, 'labels': labels, 'description': description, 'value': value}
        data.append(metric)
        if service == 'scheduler':
            date_str = json_data[service]['latest_scheduler_heartbeat'][:19]
            date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            value = date.timestamp()
            metric_name = '{0}_exporter_scheduler_seconds'.format(conf['name'])
            description = 'Latest scheduler heartbeat timestamp.'
            metric = {'metric_name': metric_name, 'labels': labels, 'description': description, 'value': value}
            data.append(metric)

def get_data_dags():
    '''Get dags list via API'''
    req = urllib.request.Request('{}/api/experimental/latest_runs'.format(conf['target_url']))
    responce = urllib.request.urlopen(req, timeout=conf['timeout'])
    raw_data = responce.read().decode()
    json_data = json.loads(raw_data)
    log.debug('get_data_dags: {}'.format(json_data))
    parse_data_dags(json_data)

def parse_data_dags(json_data):
    '''Parse dags list via API'''
    data_tmp['dags'] = list()
    for line in json_data['items']:
        data_tmp['dags'].append(line['dag_id'])
    log.debug('Got ids: {}'.format(data_tmp['dags']))

def get_data_dags_status():
    '''Get dags list via API'''
    for dag_id in data_tmp['dags']:
        url = '{}/api/experimental/dags/{}/dag_runs'.format(conf['target_url'], dag_id)
        log.debug('requesting url: {}'.format(url))
        req = urllib.request.Request(url)
        responce = urllib.request.urlopen(req, timeout=conf['timeout'])
        raw_data = responce.read().decode()
        json_data = json.loads(raw_data)
        log.debug('get_data_dags_status: {}'.format(json_data[-1]))
        parse_data_dags_status(json_data[-1])

def parse_data_dags_status(json_data):
    '''Parse dags status via API'''
    labels = dict()
    labels['dag_id'] = label_clean(json_data['dag_id'])
    labels['target_url'] = label_clean(conf['target_url'])
    metric_name = '{0}_exporter_dags_last_status'.format(conf['name'])
    description = 'Dags status: 1 = OK, 0 = Fail.'
    status = json_data['state']
    value = int(conf['dag_status_map'].get(status, -999))
    if value == -999:
        log.error('parse_data_dags_status: unknown_status: {}'.format(status))
        airflow_exporter_up.set(0)
        airflow_exporter_errors_total.inc()
    metric = {'metric_name': metric_name, 'labels': labels, 'description': description, 'value': value}
    data.append(metric)
    # add start_date
    date_str = json_data['start_date'][:19]
    date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    value = date.timestamp()
    metric_name = '{0}_exporter_dags_last_run_seconds'.format(conf['name'])
    description = 'Dag last run timestamp.'
    metric = {'metric_name': metric_name, 'labels': labels, 'description': description, 'value': value}
    data.append(metric)

def label_clean(label):
    replace_map = {
        '\\': '',
        '"': '',
        '\n': '',
        '\t': '',
        '\r': '',
        '-': '_',
        ' ': '_'
    }
    for r in replace_map:
        label = str(label).replace(r, replace_map[r])
    return label

# run
conf = dict()
get_config(args)
log = configure_logging()
data = list()
data_tmp = dict()

airflow_exporter_up = prometheus_client.Gauge('airflow_exporter_up', 'airflow exporter scrape status')
airflow_exporter_errors_total = prometheus_client.Counter('airflow_exporter_errors_total', 'exporter scrape errors total counter')

class Collector(object):
    def collect(self):
        # add static metrics
        gauge = prometheus_client.core.GaugeMetricFamily
        counter = prometheus_client.core.CounterMetricFamily
        # get dinamic data
        try:
            get_data()
            airflow_exporter_up.set(1)
        except:
            trace = traceback.format_exception(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            for line in trace:
                log.error('{0}'.format(line[:-1]))
            airflow_exporter_up.set(0)
            airflow_exporter_errors_total.inc()
        # add dinamic metrics
        to_yield = set()
        for _ in range(len(data)):
            metric = data.pop()
            labels = list(metric['labels'].keys())
            labels_values = [ metric['labels'][k] for k in labels ]
            if metric['metric_name'] not in to_yield:
                setattr(self, metric['metric_name'], gauge(metric['metric_name'], metric['description'], labels=labels))
            if labels:
                getattr(self, metric['metric_name']).add_metric(labels_values, metric['value'])
                to_yield.add(metric['metric_name'])
        for metric in to_yield:
            yield getattr(self, metric)

registry = prometheus_client.core.REGISTRY
registry.register(Collector())

prometheus_client.start_http_server(conf['listen_port'])

# endless loop
while True:
    try:
        while True:
            time.sleep(conf['check_interval'])
    except KeyboardInterrupt:
        break
    except:
        trace = traceback.format_exception(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        for line in trace:
            log.error('{0}'.format(line[:-1]))

