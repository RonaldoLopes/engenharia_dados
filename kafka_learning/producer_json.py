import json
import producer_settings
from confluent_kafka import Producer
import delivery_reports

class Kafka(object):

    @staticmethod
    def json_producer(broker, object_name, kafka_topic):

        p = Producer(producer_settings.producer_settings_json(broker))

        get_data = object_name

        for data in get_data:
            try:
                p.poll(0)
                p.producer(
                    topic=kafka_topic,
                    value=json.dumps(data).encode('utf-8'),
                    callback=delivery_reports.on_delivery_json
                )
            except BufferError:
                print("buffer full")
                p.poll(0.1)
            except ValueError:
                print("invalid input")
                raise
            except KeyboardInterrupt:
                raise
        p.flush()