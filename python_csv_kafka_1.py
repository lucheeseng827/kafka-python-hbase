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


for i in xrange(10):
	writer = avro.io.DatumWriter(schema)
	bytes_writer = io.BytesIO()
	encoder = avro.io.BinaryEncoder(bytes_writer)
	writer.write({"name": "123", "favorite_color": "111", "favorite_number": random.randint(0,10)}, encoder)
	#writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
	raw_bytes = bytes_writer.getvalue()
	producer.send_messages(topic, raw_bytes)



