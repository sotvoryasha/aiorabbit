FROM python37


COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /opt/rmq
COPY rmqclient /opt/rmq/rmqclient
WORKDIR /opt/rmq/rmqclient

EXPOSE 5672