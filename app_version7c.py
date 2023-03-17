from flask import Flask, render_template, Response #, url_for, request, redirect, jsonify, Response 
import json
import requests
import pandas as pd
import re
from datetime import timedelta, timezone
from datetime import datetime as dt
import time
import os
import calendar
from prometheus_client import start_http_server, Metric, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR , Gauge , generate_latest,CollectorRegistry
from prometheus_client import Counter, Summary, Histogram, Info  
from prometheus_flask_exporter import PrometheusMetrics 
import prometheus_client as prometheus_client
from dateutil.parser import * 
from requests.auth import HTTPBasicAuth
# from airflow.hooks.base_hook import BaseHook
# prometheus_client.REGISTRY.(prometheus_client.PROCESS_COLLECTOR)
import random as random
app = Flask(__name__)
metrics = PrometheusMetrics(app)
registry = CollectorRegistry()  # chat
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(REGISTRY._names_to_collectors["python_gc_objects_collected"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_exporter_info"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_http_request_duration_seconds_bucket"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_http_request_total"])
# REGISTRY.unregister(REGISTRY._names_to_collectors["airflow_dag_failed_count_created"])

username = os.environ.get("DB_User")
password = os.environ.get("DB_Password")

graphs = {} #           names in prometheus service  
# graphs['g'] = Gauge(name="flask_http_request", value="jakis value", )

# _INF = float("inf")
# graphs['g'] = Gauge('ingest_statistics_prod', 'Value gathered by sensor',labelnames = ['Dag_id', 'Dag_run_url',"Execution_date","Id","Run_id","Start_date","State","State_code",'Emoticons' ])
# graphs['c'] = Gauge('ingest_statistics_prod_counter', 'Value gathered by sensor',labelnames = ['Dag_id', "State"])#,"State_code" ])
###########
# graphs['g'] = Gauge('vin_whitelisting_services', 'Value gathered by sensor',labelnames = ['host', '_status',"vin","vehicle_code","car_type","status","request_time","creation_time","request_source","request_link","request_reason","contact","additional_contact","comment" ])
##########
### chat    ###
# Define Prometheus metrics
running_counter = Counter('airflow_dag_running_count', 'Number of running DAGs',labelnames = ['Dag_id', "State"])#, registry=registry)
failed_counter = Counter('airflow_dag_failed_count', 'Number of failed DAGs',labelnames = ['Dag_id', "State"])#, registry=registry)
success_counter = Counter('airflow_dag_success_count', 'Number of successful DAGs',labelnames = ['Dag_id', "State"])#, registry=registry)
airflow_dag_counter = Gauge('ingest_statistics_prod_counter','Number of successful DAGs',labelnames = ['Dag_id', "State"])#, registry=registry)
# ingest_statistics_prod_counter
### chat    ###
data_pipeline ={
        'trigger_data_pipeline':  'data_pipeline',
        'trigger_ingest':  'ingest',
        'trigger_signal_extractor':  'signal_extractor',
        'trigger_signal_synchronizer':  'signal_synchronizer',
        'trigger_endurance_run':  'endurance_run',
        'trigger_data_catalog': 'data_catalog',
        'trigger_marker_import_tablet_label': 'marker_import_tablet_label',
        'trigger_auto_labelling':   'auto_labelling',
        'trigger_video_preview': 'video_preview',
        'trigger_image_extractor':  'image_extractor',
        'trigger_video_creator': 'video_creator',
        'trigger_dq_repro_rules':   'dq_repro_rules',
        # 'dq-apps' : 'dq-apps',
    }
@app.route('/')
def hello():
    return ' chat '

### chat ###
# @app.route('/dag/<task_id>')
def airflow_restapi_dags(task_id='data_pipeline'):
    start = dt.now()

    auth = HTTPBasicAuth('user', 'password')
    # auth = HTTPBasicAuth(username, password)
    ###################  NOTHING TO CHANGE ABOVE THIS LINE   ##############

    connection = {
        "proxies" : {'http': 'socks5h://192.168.67.2:1081', 'https': 'socks5h://192.168.67.2:1081'},
        'url' :  f"https://airflow-prod.apps.devops.advantagedp.org/api/experimental/dags/{task_id}/dag_runs",
    }
    
    res7 = requests.get(connection['url'], auth=auth, proxies=connection['proxies'], verify=False) # PROXY
    # res7 = requests.get(connection['url'], auth=auth, verify=False)

    
    # print( f'res7 status {res7.status_code}')
    
    res_j = res7.json()
    # res_j = res_j[:-50]
    # print( type(res_j))
    execution_date, start_date,state,dag_id = zip(
        *(
            (
                
                item["execution_date"],
                item["start_date"],
                item["state"],   
                item["dag_id"], 
            )
            for item in res_j

        )
    )
    df = pd.DataFrame(
        {
            
            "Execution_date" :  execution_date,
            "Start_date" :  start_date,
            "State" :  state,
            "Dag_id" :  dag_id,
        }
    )

    now = dt.now(timezone.utc )
    time_threshold = now - timedelta(days=4) 
    time_threshold_failed = now - timedelta(minutes=60) 
    time_threshold_success = now - timedelta(minutes=60) 
    # if task_id != 'data_pipeline':
        # time_threshold_success = now - timedelta(days=3) 
        
    df['Execution_date'] = pd.to_datetime(df['Execution_date'])  
    df['Start_date'] = pd.to_datetime(df['Start_date'])
    # df = df[df['Start_date'] > time_threshold]
    
    running = df[(df['State']=='running') & ( df['Start_date'] > time_threshold)]
    failed = df[(df['State']=='failed')   & ( df['Start_date'] > time_threshold_failed) ]
    success = df[(df['State']=='success') & ( df['Start_date'] > time_threshold_success)]
    
    running_length = len(running)
    failed_length = len(failed)
    success_length = len(success)

    success_to_html = success.tail(10).to_html()
    failed_to_html = failed.tail(10).to_html()
    running_to_html = running.tail(10).to_html()
    df_to_html = success_to_html + failed_to_html + running_to_html
    # Increment Prometheus counters
    # running_counter.labels(task_id).inc(running_length)
    # failed_counter.labels(task_id).inc(failed_length)
    # success_counter.labels(task_id).inc(success_length)
    # running_counter.labels(Dag_id=task_id, State="running").inc(running_length)
    # failed_counter.labels(Dag_id=task_id, State="failed").inc(failed_length)
    # success_counter.labels(Dag_id=task_id, State="success").inc(success_length)

    airflow_dag_counter.labels(Dag_id=task_id, State="running").set(running_length)
    airflow_dag_counter.labels(Dag_id=task_id, State="failed").set(failed_length)
    airflow_dag_counter.labels(Dag_id=task_id, State="success").set(success_length)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="180">
            {task_id} </br>
            {stop - start} duration </br>
            {df_to_html} </br>
            """


### chat ####


@app.route('/data_pipeline')
def data_pipeline():
    s = dt.now()
    task_id='data_pipeline'
    data_pipeline ={

        'trigger_data_pipeline':  'data_pipeline',
        'trigger_ingest':  'ingest',
        'trigger_signal_extractor':  'signal_extractor',
        'trigger_signal_synchronizer':  'signal_synchronizer',
        'trigger_endurance_run':  'endurance_run',
        'trigger_data_catalog': 'data_catalog',
        'trigger_marker_import_tablet_label': 'marker_import_tablet_label',
        'trigger_auto_labelling':   'auto_labelling',
        'trigger_video_preview': 'video_preview',
        'trigger_image_extractor':  'image_extractor',
        'trigger_video_creator': 'video_creator',
        'trigger_dq_repro_rules':   'dq_repro_rules',
        # 'dq-apps' : 'dq-apps',

        }
    data_pipeline2 ={

        'dag-run-config-check': 'dag-run-config-check',
        
        'ingest-pipeline-trigger': 'ingest-pipeline-trigger',
        'ingest-pipeline-sensor': 'ingest-pipeline-sensor',
        
        'signal-extraction-pipeline-trigger': 'signal-extraction-pipeline-trigger',
        'signal-extraction-pipeline-sensor':  'signal-extraction-pipeline-sensor', 
        
        'marker-import-tablet-label-trigger': 'marker-import-tablet-label-trigger',
        'marker-import-tablet-label-sensor': 'marker-import-tablet-label-sensor',
        
        'video-preview-trigger':  'video-preview-trigger', 
        
        'image-extractor-trigger': 'image-extractor-trigger',
        'image-extractor-sensor': 'image-extractor-sensor',
        
        'dq-repro-rules-trigger': 'dq-repro-rules-trigger',
        'dq-repro-rules-sensor': 'dq-repro-rules-sensor',
        
        'auto-labelling-trigger': 'auto-labelling-trigger',
        'auto-labelling-sensor': 'auto-labelling-sensor',
        
        'video-creator-trigger':  'video-creator-trigger', 
        'video-creator-sensor': 'video-creator-sensor',
        
        'dag-run-status':'dag-run-status'

        }
    show_df =airflow_restapi_dags(data_pipeline['trigger_data_pipeline'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_ingest'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_signal_extractor'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_signal_synchronizer'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_endurance_run'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_data_catalog'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_marker_import_tablet_label'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_auto_labelling'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_video_preview'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_image_extractor'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_video_creator'])
    show_df+=airflow_restapi_dags(data_pipeline['trigger_dq_repro_rules'])
    # for key , task in list(data_pipeline.items()):
    #     # start = dt.now()
    #     show_df =  airflow_restapi_dags(data_pipeline[key])
    #     # show_df = airflow_restapi_dags(task_id)
    
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    st = dt.now()
    return f"""<meta http-equiv="refresh" content="60">
            {st - s} duration </br>
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 

@app.route('/metrics')
def prom_metrics():
    return Response(generate_latest(REGISTRY), mimetype='text/plain; version=0.0.4; charset=utf-8')


def time_show():
    start = dt.now()

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return start,now,stop 



if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0") 
    # airflow_restapi()
