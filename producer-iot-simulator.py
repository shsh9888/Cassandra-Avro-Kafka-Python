from cassandra.cluster import Cluster
import  sys
import time
import io
import random
import avro.schema
from avro.io import DatumWriter
from kafka import SimpleProducer
from kafka import KafkaClient
import uuid

# To send messages synchronously
KAFKA = KafkaClient('localhost:9092')
PRODUCER = SimpleProducer(KAFKA)

# Kafka topic
TOPIC = "test"

# Path to user.avsc avro schema
SCHEMA_PATH = "iot.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

##Default values
buildingName = 'All' #send the devices from all the buildimng
numberOfEvents = 10 ## number of events per device
buildings =[]
devices =[]


if len(sys.argv) == 2:
    buildingName = sys.argv[1] if  sys.argv[1]  else 'All'
if len(sys.argv) == 3:
    numberOfEvents = sys.argv[2]  if sys.argv[2] else 10

cluster = Cluster()
session = cluster.connect('iot')

if buildingName == 'All':
    buildings = session.execute("SELECT buildingId FROM buildinginfo")
else:
    query = "SELECT buildingId FROM buildinginfo where buildingname='{}' ALLOW FILTERING".format(buildingName)
    buildings = session.execute(query)

for building in buildings:
    query = "SELECT * FROM deviceinfo where buildingId={} ALLOW FILTERING".format(building.buildingid)
    print(query)
    devicesResults = session.execute(query)
    for item in devicesResults:
        devices.append(item)

print(devices)
categorical = ["on", "off", "disabled"]
while True:
    for device in devices:
        for i in range(numberOfEvents):
            data = random.choice(categorical) if device.devicetype == "Categorical" else random()
            data = str(data)
            deviceData = {"id" : str(uuid.uuid1()), "deviceId": str(device.deviceid), "data":data, "createdTimestamp" : int(time.time())}
            print("sending device data",deviceData)
            writer = DatumWriter(SCHEMA)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(deviceData, encoder)
            raw_bytes = bytes_writer.getvalue()
            PRODUCER.send_messages(TOPIC, raw_bytes)

    time.sleep(1)


