import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumReader, DatumWriter

schema1 = avro.schema.Parse(open("iot.avsc", "rb").read())

writer = DataFileWriter(open("dataset.avro", "wb"), DatumWriter(), schema1)
writer.append({"id": "1", "deviceId": 'asdsf', "data": "233", "createdTimestamp": 123})
writer.close()

reader = DataFileReader(open("dataset.avro", "rb"), DatumReader())
for user in reader:
    print (user)
reader.close()
