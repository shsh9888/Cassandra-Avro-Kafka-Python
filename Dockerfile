FROM ubuntu
MAINTAINER Shravan <shsh9888@colorado.edu>
RUN mkdir -p /srv
WORKDIR /srv

COPY entrypoint.sh /srv
COPY producer-iot-simulator.py /srv
COPY iot.avsc /srv
COPY iot.avsc .
RUN chmod 777 /srv/producer-iot-simulator.py
RUN chmod 777 /srv/entrypoint.sh
RUN chmod 777 /srv/iot.avsc
RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install kafka-python && pip3 install cassandra-driver && pip3 install avro-python3
ENTRYPOINT ["/srv/entrypoint.sh"]
