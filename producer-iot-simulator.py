from cassandra.cluster import Cluster
import  sys
import time
import io
import random
from json import dumps
import avro.schema
from avro.io import DatumWriter
from kafka import KafkaProducer
from kafka import KafkaClient
import uuid

# To send messages synchronously
# KAFKA = KafkaClient('kafka:9092')
PRODUCER = KafkaProducer(bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x:dumps(x).encode('utf-8'))

# Kafka topic
TOPIC = 'test'

# Path to user.avsc avro schema
SCHEMA_PATH = "iot.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

##Default values
# buildingName = 'All' #send the devices from all the buildimng
numberOfEvents = 10 ## number of events per device
buildings =[]
devices =[]


if len(sys.argv) > 1:
    numberOfEvents = sys.argv[1]  if sys.argv[1] else 10

numberOfEvents = int(numberOfEvents)

print("final number of events", numberOfEvents)

cluster = Cluster(['cassandra'],port=9042)
session = cluster.connect('iot')

# if buildingName == 'All':
buildings = session.execute("SELECT buildingId FROM buildinginfo")
# else:
#     query = "SELECT buildingId FROM buildinginfo where buildingname='{}' ALLOW FILTERING".format(buildingName)
#     buildings = session.execute(query)

for building in buildings:
    query = "SELECT * FROM deviceinfo where buildingId={} ALLOW FILTERING".format(building.buildingid)
    devicesResults = session.execute(query)
    for item in devicesResults:
        devices.append(item)

categorical = ["on", "off", "disabled"]
while True:
    for device in devices:
        for i in range(numberOfEvents):
            data = random.choice(categorical) if device.devicetype == "Categorical" else random.random()
            data = str(data)
            deviceData = {"id" : str(uuid.uuid1()), "deviceId": str(device.deviceid), "data":data, "createdTimestamp" : int(time.time())}
            print("sending device data",deviceData)
            writer = DatumWriter(SCHEMA)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(deviceData, encoder)
            raw_bytes = bytes_writer.getvalue()
            PRODUCER.send(TOPIC, value=deviceData)



