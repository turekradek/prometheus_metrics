import re 
import requests 
from requests.auth import HTTPBasicAuth
from datetime import datetime as dt

def run_session_gate_dag(airflow_url, username, password, dag_id , conf, run_id):
    auth = HTTPBasicAuth(username, password)
    # TO LOCAL TESTS
    data = {
        'conf': conf,
        "run_id": run_id,
    }
    connection = {
        "proxies" : {'http': 'socks5h://192.168.67.2:1081', 'https': 'socks5h://192.168.67.2:1081'},# ONLY FOR LOCAL TRIGGER
        'url_ingest' :  "https://airflow-prod.apps.devops.advantagedp.org/api/experimental/dags/session_checker_single_session_reingest/dag_runs",
        # "pods" : f"{airflow_url}/api/experimental/dags/devops_monitoring_failed_pods/dag_runs",
        "dag_url" : f"{airflow_url}/api/experimental/dags/{dag_id}/dag_runs",
        'data': data,
    }
    try:
        # res7 = requests.post(connection['dag_url'],auth=auth,proxies=connection['proxies'], json=connection['data'], verify=False) # RUN DAG , json={} NECESSARY TO RUN DAG
        res7 = requests.post(connection['dag_url'],auth=auth,json=connection['data'], verify=False) # RUN DAG , json={} NECESSARY TO RUN DAG
    except  requests.exceptions.HTTPError as err:
        print( f'Bad Status Code {res7.status_code}')
    
    print( res7.status_code )
    res7_json = res7.json()
    print(res7_json)
    return res7.json()


# airflow_url = "https://airflow-prod.apps.devops.advantagedp.org/"
airflow_url = "https://airflow-stg.apps.devops.advantagedp.org/"
username = 'ingest_pipeline-s'
password = '************'
dag_id = 'session_gate'
conf = {
        "sessionQualifierResult": {"sessionId": "1bdb2581-5419-4d96-a1e8-0185c54ed9f7",
        "ingestable": "TRUE",
        "timestamp": 1675961395000,
        "qualifierTags": {"session-checker.car-config.bordnet-version.supported": "TRUE",
        "session-checker.car-config.software-version.available": "TRUE",
        "session-checker.loggers.logger-count.valid": "TRUE",
        "session-checker.metadata.session-id.valid": "TRUE",
        "session-checker.metadata.vin.valid": "TRUE",
        "session-checker.streams.continuous.file-indices.no-overflow": "TRUE",
        "session-checker.streams.continuous.file-indices.unique": "TRUE",
        "session-checker.streams.continuous.files.complete": "TRUE",
        "session-checker.streams.continuous.last-file.exist": "TRUE",
        "session-checker.streams.continuous.last-file.unique": "TRUE",
        "session-checker.streams.continuous.number-of-files.no-duplicate": "TRUE",
        "session-checker.streams.recording-type.consistent": "TRUE",
        "session-checker.streams.triggered.file-indices.no-overflow": "TRUE",
        "session-checker.streams.triggered.file-indices.unique": "TRUE",
        "session-checker.streams.triggered.files.complete": "TRUE",
        "session-checker.streams.triggered.last-file.exist": "TRUE",
        "session-checker.streams.triggered.last-file.unique": "TRUE",
        "session-checker.streams.triggered.number-of-files.no-duplicate": "TRUE"},
        "qualifierExpression": "session-checker.car-config.bordnet-version.supported && session-checker.car-config.software-version.available && session-checker.loggers.logger-count.valid && session-checker.metadata.session-id.valid && session-checker.metadata.vin.valid && session-checker.streams.continuous.file-indices.no-overflow && session-checker.streams.continuous.file-indices.unique && session-checker.streams.continuous.files.complete && session-checker.streams.continuous.last-file.exist && session-checker.streams.continuous.last-file.unique && session-checker.streams.continuous.number-of-files.no-duplicate && session-checker.streams.recording-type.consistent && session-checker.streams.triggered.file-indices.no-overflow && session-checker.streams.triggered.file-indices.unique && session-checker.streams.triggered.files.complete && session-checker.streams.triggered.last-file.exist && session-checker.streams.triggered.last-file.unique && session-checker.streams.triggered.number-of-files.no-duplicate",
        "qualifierTimeout": 86400,
        "qualifierExpressionVersion": "1.5.0-RC662"},
        "session_id": "1bdb2581-5419-4d96-a1e8-0185c54ed9f7"
        }
time_now = dt.now().isoformat('Z')
run_id = f"{dag_id}_{time_now}_{conf['session_id']}_reprocessingg"      
run_session_gate_dag(airflow_url, username, password, dag_id , conf, run_id )


# curl -u ingest_pipeline-s:1Rt.d5WssojT.2 -X POST https://airflow-stg.apps.devops.advantagedp.org/api/experimental/dags/devops_monitoring_failed_pods/dag_runs -d {}
# curl -u username:password -X POST https://airflow-stg.apps.devops.advantagedp.org/api/experimental/dags/devops_monitoring_failed_pods/dag_runs -d {}
# -d {} # IS NECESSARY 