FROM python:3.7-buster

RUN apt update -y && apt upgrade -y
RUN apt-get install -y openjdk-11-jdk
RUN apt-get update && apt-get install -y \
  libxml2-dev \
  libxslt1-dev \
  python3-dev \
  zlib1g-dev \
  && pip install --upgrade pip \
  && pip install virtualenv \
  && rm -rf /var/cache/apk/*

RUN wget -q https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
RUN tar -xzf spark-3.3.2-bin-hadoop3.tgz
RUN mv spark-3.3.2-bin-hadoop3 /opt/spark
RUN rm -r spark-3.3.2-bin-hadoop3.tgz
RUN ln -s /opt/spark-3.3.2 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN pip install jupyter

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

