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
from prometheus_client import start_http_server, Metric, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR , Gauge
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
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(REGISTRY._names_to_collectors["python_gc_objects_collected"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_exporter_info"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_http_request_duration_seconds_bucket"])
REGISTRY.unregister(REGISTRY._names_to_collectors["flask_http_request_total"])

username = os.environ.get("DB_User")
password = os.environ.get("DB_Password")

graphs = {} #           names in prometheus service  
# graphs['g'] = Gauge(name="flask_http_request", value="jakis value", )

# _INF = float("inf")
graphs['g'] = Gauge('ingest_statistics_prod', 'Value gathered by sensor',labelnames = ['Dag_id', 'Dag_run_url',"Execution_date","Id","Run_id","Start_date","State","State_code",'Emoticons' ])
graphs['c'] = Gauge('ingest_statistics_prod_counter', 'Value gathered by sensor',labelnames = ['Dag_id', "State"])#,"State_code" ])
###########
# graphs['g'] = Gauge('vin_whitelisting_services', 'Value gathered by sensor',labelnames = ['host', '_status',"vin","vehicle_code","car_type","status","request_time","creation_time","request_source","request_link","request_reason","contact","additional_contact","comment" ])
##########
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
    return 'CONNECTED'


# @app.route('/dag')
def airflow_restapi():
    start_all = dt.now()
    
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
    
    # temporary = ':) '
    auth = HTTPBasicAuth('user', 'password')
    proxies = {'http': 'socks5h://192.168.67.2:1081', 'https': 'socks5h://192.168.67.2:1081'}
    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    # temporary = ''
    # airflow_restapi_dags(data_pipeline['trigger_data_pipeline'])
    # airflow_restapi_dags(data_pipeline['trigger_ingest'])
    # airflow_restapi_dags(data_pipeline['trigger_signal_extractor'])
    # airflow_restapi_dags(data_pipeline['trigger_signal_synchronizer'])
    # airflow_restapi_dags(data_pipeline['trigger_endurance_run'])
    # airflow_restapi_dags(data_pipeline['trigger_data_catalog'])
    # airflow_restapi_dags(data_pipeline['trigger_marker_import_tablet_label'])
    # airflow_restapi_dags(data_pipeline['trigger_auto_labelling'])
    # airflow_restapi_dags(data_pipeline['trigger_video_preview'])
    # airflow_restapi_dags(data_pipeline['trigger_image_extractor'])
    # airflow_restapi_dags(data_pipeline['trigger_video_creator'])
    # airflow_restapi_dags(data_pipeline['trigger_dq_repro_rules'])
    for key , task in list(data_pipeline.items()):
        # start = dt.now()
        airflow_restapi_dags(data_pipeline[key])
        # stop = dt.now()
    # connection = {
    #     "proxies" : {'http': 'socks5h://192.168.67.2:1081', 'https': 'socks5h://192.168.67.2:1081'},
    #     'url' :  f"https://airflow-prod.apps.devops.advantagedp.org/api/experimental/dags/data_pipeline/dag_runs",
    # }
    # # # url =  f"https://airflow-prod.apps.devops.advantagedp.org/api/experimental/dags/data_pipeline/dag_runs",
    # res7 = requests.get(connection['url'], auth=auth, proxies=connection['proxies'], verify=False) # PROXY
    # # # res7 = requests.get(connection['url'], auth=auth, verify=False)
    # res_j = res7.json()
    # res_j = res_j[:-50]
    # print( type(res_j))

    stop_all = dt.now()     
    # return f'''CHOLERA  CO JEST <br>
    #         {res_j}'''
    return ' func airflow_restapi '
    # return f"""
    #         <meta http-equiv="refresh" content="120">
    #         startall - stopall {stop_all - start_all}<br>
    #         {now} <br>
    # #     """  # start - stop {stop - start}<br>  {temporary}
        
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
    execution_date, start_date,state = zip(
        *(
            (
                item["execution_date"],
                item["start_date"],
                item["state"],       
            )
            for item in res_j

        )
    )
    df = pd.DataFrame(
        {
            "Execution_date" :  execution_date,
            "Start_date" :  start_date,
            "State" :  state,
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

    # running = df[df['State']=='running']
    # # failed = df[df['State']=='failed' ]
    # # failed = df[(df['State']=='failed') & ( df['Start_date'] > time_threshold_failed) ]
    # success = df[df['State']=='success']
    success_to_html = success.tail(3).to_html()
    failed_to_html = failed.tail(3).to_html()
    running_to_html = running.tail(3).to_html()

    graphs['c'].labels(
        Dag_id=task_id,
        State="running",
        # State_code=value["State_code"],
        ).set(running_length)
    graphs['c'].labels(
            Dag_id=task_id,
            State='failed',
            # State_code=value["State_code"],
        ).set(failed_length)
    graphs['c'].labels(
            Dag_id=task_id,
            State="success",
            # State_code=value["State_code"],
            ).set(success_length)
    # temporary = df.tail().to_html() #success_to_html + failed_to_html + running_to_html# +  success_ingest_to_html + failed_ingest_to_html + running_ingest_to_html
    # memory_usage = df.memory_usage(deep=True).sum()
    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    # print( df.head(10) )
    # df_to_html = df.head().to_html()
    # df_to_html = success_to_html + failed_to_html + running_to_html
    df_to_html = success_to_html + failed_to_html + running_to_html
    return df_to_html
    # return f''' {task_id} - DONE {stop - start} NOW {now}'''
    # # return  f"""
    # #         <meta http-equiv="refresh" content="120">   
    # #         {now} <br>
    # #         {df.to_html()}
    # #         <br>
    # #        """

@app.route('/data_pipeline')
def data_pipeline():
    task_id='data_pipeline'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """ 

@app.route('/ingest')
def ingest():
    task_id='ingest'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
##
@app.route('/ingest_pipeline')
def ingest_pipeline():
    task_id='ingest_pipeline'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
##
@app.route('/signal_extractor')
def signal_extractor():
    task_id='signal_extractor'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """     

@app.route('/signal_synchronizer')
def signal_synchronizer():
    task_id='signal_synchronizer'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   

@app.route('/endurance_run')
def endurance_run():
    task_id='endurance_run'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
            
@app.route('/data_catalog')
def data_catalog():
    task_id='data_catalog'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   

@app.route('/marker_import_tablet_label')
def marker_import_tablet_label():
    task_id='marker_import_tablet_label'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   

@app.route('/auto_labelling')
def auto_labelling():
    task_id='auto_labelling'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
            
@app.route('/video_preview')
def video_preview():
    task_id='video_preview'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
            
            
@app.route('/image_extractor')
def image_extractor():
    task_id='image_extractor'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """   
            
@app.route('/video_creator')
def video_creator():
    task_id='video_creator'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """            

@app.route('/dq_repro_rules')
def dq_repro_rules():
    task_id='dq_repro_rules'
    show_df = airflow_restapi_dags(task_id)
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return f"""<meta http-equiv="refresh" content="60">
            {now} <br>
            {stop - start}<br>
            {show_df} <br>
            """            


def time_show():
    start = dt.now()

    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    stop = dt.now()
    return start,now,stop 

# @app.route('/xx')
# def xx(task_id='data_pipeline'):
#     # start = dt.now()

#     # now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
#     # stop = dt.now()
#     show_time = time_show()
#     print(f' xx   {show_time} '  )
#     start, now, stop = show_time[0], show_time[1] , show_time[2]
#     # return df_to_html
#     return  f"""
#             <meta http-equiv="refresh" content="10">   
#             {now} <br>
#             {stop - start}<br>
#             <br>
#             """
            
# @app.route('/yy')
# def yy(task_id='data_pipeline'):
#     # start = dt.now()

#     # now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
#     # stop = dt.now()
#     show_time = time_show()
#     print(f' xx   {show_time} '  )
#     start, now, stop = show_time[0], show_time[1] , show_time[2]
#     # return df_to_html
#     return  f"""
#             <meta http-equiv="refresh" content="5">   
#             {now} <br>
#             {stop - start}<br>
#             <br>
#             """
# print( f'''{type(xx())} 
#     {xx()} ''')
            
            
@app.route('/metrics')
def requets_count_counter():
    """Prepare metrics for prometheus

    Returns:
        _type_: _description_ prometheus metric object
    """
    # all_metrics = []
    # for key in graphs.keys():
    #     all_metrics.append(graphs[key])
    # print( all_metrics[:20], all_metrics[len(all_metrics)-20:] )
    # matrics_list = []
    # now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    # for k , v in graphs.items():
    #     matrics_list.append(prometheus_client.generate_latest(v))
    # matrics_list = set(matrics_list)
    # return Response(matrics_list, mimetype='application/json')
    show_time = time_show()
    start, now, stop = show_time[0], show_time[1] , show_time[2]
    return Response(prometheus_client.generate_latest(graphs['c']), mimetype='application/json')
    # return Response(prometheus_client.generate_latest(all_metrics), mimetype='application/json')



if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0") 
    # airflow_restapi()
