FROM python:3.9-slim-buster

RUN apt-get update && apt-get install nano

RUN /usr/local/bin/python -m pip install --upgrade pip

WORKDIR /app

COPY requirements.txt requirements.txt

RUN  pip install -r requirements.txt


COPY Dockerfile .
COPY requirements.txt . 
COPY app_version4.py .
COPY app_version5.py .
COPY app_version7c.py .
COPY run_session_gate_dag.py .

EXPOSE 8080
# configure the container to run in an executed manner
ENTRYPOINT [ "python" ]
CMD ["app_version7c.py"]

