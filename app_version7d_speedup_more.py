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
# data_pipeline ={
#         'trigger_data_pipeline':  'data_pipeline',
#         'trigger_ingest':  'ingest',
#         'trigger_signal_extractor':  'signal_extractor',
#         'trigger_signal_synchronizer':  'signal_synchronizer',
#         'trigger_endurance_run':  'endurance_run',
#         'trigger_data_catalog': 'data_catalog',
#         'trigger_marker_import_tablet_label': 'marker_import_tablet_label',
#         'trigger_auto_labelling':   'auto_labelling',
#         'trigger_video_preview': 'video_preview',
#         'trigger_image_extractor':  'image_extractor',
#         'trigger_video_creator': 'video_creator',
#         'trigger_dq_repro_rules':   'dq_repro_rules',
#         # 'dq-apps' : 'dq-apps',
#     }
# data_pipeline2 ={

#         'dag-run-config-check': 'dag-run-config-check',
        
#         'ingest-pipeline-trigger': 'ingest-pipeline-trigger',
#         'ingest-pipeline-sensor': 'ingest-pipeline-sensor',
        
#         'signal-extraction-pipeline-trigger': 'signal-extraction-pipeline-trigger',
#         'signal-extraction-pipeline-sensor':  'signal-extraction-pipeline-sensor', 
        
#         'marker-import-tablet-label-trigger': 'marker-import-tablet-label-trigger',
#         'marker-import-tablet-label-sensor': 'marker-import-tablet-label-sensor',
        
#         'video-preview-trigger':  'video-preview-trigger', 
        
#         'image-extractor-trigger': 'image-extractor-trigger',
#         'image-extractor-sensor': 'image-extractor-sensor',
        
#         'dq-repro-rules-trigger': 'dq-repro-rules-trigger',
#         'dq-repro-rules-sensor': 'dq-repro-rules-sensor',
        
#         'auto-labelling-trigger': 'auto-labelling-trigger',
#         'auto-labelling-sensor': 'auto-labelling-sensor',
        
#         'video-creator-trigger':  'video-creator-trigger', 
#         'video-creator-sensor': 'video-creator-sensor',
        
#         'dag-run-status':'dag-run-status'

#         }
@app.route('/')
def hello():
    return ' chat '

### chat ###
# @app.route('/dag/<task_id>')
def airflow_restapi_dags(task_id='data_pipeline'):#, rows=1000):
    """
    Retrieves the status of Airflow DAG runs and updates Prometheus metrics.
    
    This function queries the Airflow REST API to get the status of DAG runs 
    and sets Prometheus metrics for the number of successful, failed, and 
    running DAGs.
    
    Args:
        task_id (str, optional): The task ID of the data pipeline. Defaults to 'data_pipeline'.
        rows (int, optional): The number of rows to retrieve. Defaults to 1000.

    Returns:
        str: The task ID of the data pipeline.
    """
    #
    connection = {
        "proxies": {'http': 'socks5h://192.168.67.2:1081', 'https': 'socks5h://192.168.67.2:1081'},
        'url': f"https://airflow-prod.apps.devops.advantagedp.org/api/experimental/dags/{task_id}/dag_runs",
    }

    with requests.Session() as session:
        session.auth = auth
        session.verify = False
        try:
            # res = session.get(connection['url'], proxies=connection['proxies'])#, params={"limit": rows})
            res = session.get(connection['url'])
        except requests.exceptions.Timeout:
            return "Error: Request timed out", 504

        res_j = res.json()
    now = dt.now(timezone.utc )
    time_threshold = now - timedelta(days=4)
    time_threshold_failed = now - timedelta(minutes=20)
    time_threshold_success = now - timedelta(minutes=20)

    running = [(item["execution_date"], item["start_date"], item["state"], item["dag_id"])
            for item in res_j if item["state"] == 'running' and dt.fromisoformat(item["start_date"]) > time_threshold]
    failed = [(item["execution_date"], item["start_date"], item["state"], item["dag_id"])
            for item in res_j if item["state"] == 'failed' and dt.fromisoformat(item["start_date"]) > time_threshold_failed]
    success = [(item["execution_date"], item["start_date"], item["state"], item["dag_id"])
            for item in res_j if item["state"] == 'success' and dt.fromisoformat(item["start_date"]) > time_threshold_success]

    # running_length = sum(item[2] == "running" for item in running)
    running_length = sum(map(lambda item: item[2] == "running", running))
    # failed_length = sum(item[2] == "failed" for item in failed)
    failed_length = sum(map(lambda item: item[2] == "failed", failed))
    # success_length = sum(item[2] == "success" for item in success)
    success_length = sum(map(lambda item: item[2] == "success", success))
    
    airflow_dag_counter.labels(Dag_id=task_id, State="running").set(running_length)
    airflow_dag_counter.labels(Dag_id=task_id, State="failed").set(failed_length)
    airflow_dag_counter.labels(Dag_id=task_id, State="success").set(success_length)
    return task_id


### chat ####


@app.route('/data_pipeline')
def data_pipeline():
    """
    Flask endpoint for the data pipeline status page.

    This function serves as the Flask endpoint for the data pipeline status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
        the result of the `airflow_restapi_dags` function.
    """
    start = dt.now()

    
    # ... (The same pattern of documentation is added to all other functions)
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
    
    show_df =airflow_restapi_dags(task_id)
    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 

@app.route('/ingest')
def ingest():
    """
    Flask endpoint for the ingest status page.

    This function serves as the Flask endpoint for the ingest status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
        the result of the `airflow_restapi_dags` function.
    """
    start = dt.now()
    task_id='ingest'
    show_df =airflow_restapi_dags(task_id)
    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {dt.now() - start}<br>
            {show_df} <br>
            # """ 

@app.route('/signal_extractor')
def signal_extractor():
    """
    Flask endpoint for the signal_extractor status page.

    This function serves as the Flask endpoint for the signal_extractor status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
        the result of the `airflow_restapi_dags` function.
    """
    start = dt.now()
    task_id='signal_extractor'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 

@app.route('/signal_synchronizer')
def signal_synchronizer():
    """
    Flask endpoint for the signal_synchronizer status page.

    This function serves as the Flask endpoint for the signal_synchronizer status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
        the result of the `airflow_restapi_dags` function.
    """
    start = dt.now()
    task_id='signal_synchronizer'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 
            

@app.route('/endurance_run')
def endurance_run():
    """
    Flask endpoint for the endurance_run status page.

    This function serves as the Flask endpoint for the endurance_run status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
            the result of the `airflow_restapi_dags` function.
    """
    
    start = dt.now()
    task_id='endurance_run'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 

@app.route('/data_catalog')
def data_catalog():
    start = dt.now()
    task_id='data_catalog'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """ 
            
@app.route('/marker_import_tablet_label')
def marker_import_tablet_label():
    start = dt.now()
    task_id='marker_import_tablet_label'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """            
            
@app.route('/auto_labelling')
def auto_labelling():
    start = dt.now()
    task_id='auto_labelling'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """   

@app.route('/video_preview')
def video_preview():
    start = dt.now()
    task_id='video_preview'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """   

@app.route('/image_extractor')
def image_extractor():
    start = dt.now()
    task_id='image_extractor'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """   
            
@app.route('/video_creator')
def video_creator():
    start = dt.now()
    task_id='video_creator'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """   
            
@app.route('/dq_repro_rules')
def dq_repro_rules():
    """
    Flask endpoint for the dq_repro_rules status page.

    This function serves as the Flask endpoint for the dq_repro_rules status page.
    It calls the `airflow_restapi_dags` function to fetch the status of the DAG runs
    and displays the results on the page. The page is set to refresh every 60 seconds.

    Returns:
        str: An HTML string containing the duration, current time, elapsed time, and
            the result of the `airflow_restapi_dags` function.
    """
    start = dt.now()
    task_id='dq_repro_rules'

    show_df =airflow_restapi_dags(task_id)

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return f"""<meta http-equiv="refresh" content="90">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            # """  
            
@app.route('/metrics')
def prom_metrics():
    """
    Flask endpoint for Prometheus metrics.

    This function serves as the Flask endpoint for exposing the Prometheus metrics.
    It generates the latest metrics from the global Prometheus registry and returns
    them in the Prometheus exposition format.

    Returns:
        Response: A Flask response object containing the Prometheus metrics in the
                text/plain MIME type with version 0.0.4 and charset utf-8.
    """
    return Response(generate_latest(REGISTRY), mimetype='text/plain; version=0.0.4; charset=utf-8')


def time_show():
    start = dt.now()

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return start,now,stop 



if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0") 
    # airflow_restapi()
