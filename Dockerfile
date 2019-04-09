FROM 675869518239.dkr.ecr.eu-central-1.amazonaws.com/ci-python37:latest


COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /opt/rmq
COPY rmqclient /opt/rmq/rmqclient
WORKDIR /opt/rmq/rmqclient

EXPOSE 5672