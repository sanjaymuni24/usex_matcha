import json
from sqlalchemy import create_engine
from confluent_kafka import Consumer, KafkaException, KafkaError,DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import pandas as pd
import time
import  traceback
import json
def connect_to_data_source(data_source):
    """
    Connect to the specified data source based on its type and connection parameters.
    """
    datasource_type = data_source.datasource_type
    connection_params = data_source.connection_params

    if datasource_type == 'Postgres':
        # Build the connection string for Postgres
        connection_string = f"postgresql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        print("Postgres connection string:", connection_string)
        connection= create_engine(connection_string).connect()
        connection.execute("SELECT 1")  # Test the connection
        print("Postgres connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasource_type == 'Mysql':
        # Build the connection string for MySQL
        connection_string = f"mysql+pymysql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        connection= create_engine(connection_string).connect()
        connection.execute("SELECT 1")  # Test the connection
        print("Postgres connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasource_type == 'Kafka':
        # Kafka connection logic (placeholder, as SQLAlchemy is not used for Kafka)
        # Kafka connection logic
        brokers = connection_params.get('brokers')
        topic = connection_params.get('topic')
        group_id = connection_params.get('group_id', 'test-group')  # Default group ID if not provided
        
        if not brokers or not topic:
            raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

        consumer_config = {
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start reading from the earliest message
            'enable.auto.commit': False      # Disable auto-commit
        }

        consumer = Consumer(consumer_config)

        try:
            # Subscribe to the topic
            consumer.subscribe([topic])
            print(f"Subscribed to Kafka topic: {topic}")

            # Poll for a message to validate the connection
            msg = consumer.poll(timeout=5.0)  # Timeout in seconds

            if msg is None:
                print("Kafka connection successful, but no messages available.")
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                print(f"Kafka connection successful. Received message: {msg.value().decode('utf-8')}")

        finally:
            # Close the consumer without committing offsets
            consumer.close()
            print("Kafka consumer closed.")
        # raise NotImplementedError("Kafka connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'RabbitMQ':
        # RabbitMQ connection logic (placeholder, as SQLAlchemy is not used for RabbitMQ)
        raise NotImplementedError("RabbitMQ connection is not implemented.")

    elif datasource_type == 'S3':
        # S3 connection logic (placeholder, as SQLAlchemy is not used for S3)
        raise NotImplementedError("S3 connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'Hive':
        # Hive connection logic (placeholder, as SQLAlchemy is not used for Hive)
        raise NotImplementedError("Hive connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'HDFS':
        # HDFS connection logic (placeholder, as SQLAlchemy is not used for HDFS)
        raise NotImplementedError("HDFS connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'CSV':
        # CSV connection logic (placeholder, as SQLAlchemy is not used for CSV)
        raise NotImplementedError("CSV connection is not implemented with SQLAlchemy.")

    else:
        raise ValueError(f"Unsupported data source type: {datasource_type}")


def query_dataset(datasource):
    """
    Query data from Postgres or Kafka based on the data source type and return the results.
    """
    
    
      # Fetch the data source from the database

    try:
        if datasource.datasource_type == 'Postgres':
            # Query data from Postgres
            connection_string = f"postgresql://{datasource.connection_params['username']}:{datasource.connection_params['password']}@{datasource.connection_params['host']}:{datasource.connection_params['port']}/{datasource.connection_params['database']}"
            engine = create_engine(connection_string)
            query = datasource.connection_params['query']  # Replace with your query
            if 'LIMIT' not in query.upper():
                query = f"{query.strip().rstrip(';')} LIMIT 5;"
            df = pd.read_sql_query(query, engine)  # Use Pandas to execute the query
            results = df.to_dict(orient='records')  # Convert the results to a list of dictionaries

        elif datasource.datasource_type == 'Kafka':
            # Kafka consumer configuration
            brokers = datasource.connection_params['brokers']
            topic = datasource.connection_params['topic']
            group_id = datasource.connection_params.get('group_id', 'query_group')
            schema_registry_url = datasource.connection_params.get('schema_registry_url')
            if not brokers or not topic:
                raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

            consumer_config = {
                'bootstrap.servers': brokers,
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }
            # Check if Avro deserialization is needed
            # Use Avro deserializer if use_avro is True
            use_avro = False
            if schema_registry_url:
                schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
                subjects = schema_registry_client.get_subjects()
                print(subjects)
                if f"{topic}-value" in subjects:
                    use_avro = True
                    print(f"Using Avro deserializer for topic: {topic}")
            if use_avro:
                
                if not schema_registry_url:
                    raise ValueError("Schema Registry URL is required for Avro deserialization.")

                schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
                avro_deserializer = AvroDeserializer(schema_registry_client)

                key_deserializer = StringDeserializer('utf_8')
                value_deserializer = avro_deserializer
                
            else:
                # Use normal JSON deserialization
                key_deserializer = StringDeserializer('utf_8')
                value_deserializer = StringDeserializer('utf_8')
                consumer = Consumer(consumer_config)

            # Create the Kafka consumer
            # Create the DeserializingConsumer
            consumer_config.update({
                'key.deserializer': key_deserializer,
                'value.deserializer': value_deserializer,
            })
            consumer = DeserializingConsumer(consumer_config)
            consumer.subscribe([topic])

            messages = []
            timeout = 5  # Timeout in seconds
            start_time = time.time()

            while time.time() - start_time < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"End of partition event: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                        break
                    else:
                        raise KafkaException(msg.error())
                else:
                    # Deserialize the message key and value
                    # key = key_deserializer(msg.key(), None)
                    
                    # value = value_deserializer(msg.value(), None)
                    # Deserialize the message key and value
                    key = msg.key()
                    value = msg.value()
                    print(f"Key: {key}")
                    print(f"Value: {msg.value()}")
                    print(f"Received message: key={key}, value={value}")
                    try:
                        if use_avro:
                            messages.append(value)  # Avro deserialized value is already a dictionary
                        else:
                            messages.append(json.loads(value))  # Parse JSON string into a dictionary
                    except json.JSONDecodeError:
                        messages.append({'message': value})

            consumer.close()
            results = messages

        else:
            return {'success': False, 'error': 'Unsupported data source type.'}

        return {'success': True, 'results': results}

    except Exception as e:
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

