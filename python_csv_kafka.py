from kafka import SimpleProducer, KafkaClient
import avro.schema
import io, random
from avro.io import DatumWriter

# To send messages synchronously
kafka = KafkaClient('localhost:9092,localhost:9093')
producer = SimpleProducer(kafka)

# Kafka topic
topic = "test"

# Path to user.avsc avro schema
schema_path="user.avsc"
schema = avro.schema.parse(open(schema_path).read())


for i in xrange(100):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    #writer.write("name", encoder)
    writer.write({"name":"{!r}".format(i),
                  "favorite_color": str(random.randint(0,100)),
                  "favorite_number": str(random.randint(0,100))},
                 encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send_messages(topic, raw_bytes)
