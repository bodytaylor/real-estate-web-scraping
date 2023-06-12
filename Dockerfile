FROM jupyter/pyspark-notebook:latest

WORKDIR /code
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


COPY . /code