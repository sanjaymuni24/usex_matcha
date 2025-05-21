from django.db import models
from django.core.exceptions import ValidationError
# Create your models here.
class DataSource(models.Model):
    """
    Model representing a data source.
    """
    DATASOURCE_TYPES = [
        ('Postgres', 'postgres'),
        ('Mysql', 'Mysql'),
        ('Kafka', 'Kafka'),
        ('RabbitMQ', 'RabbitMQ'),
        ('S3', 'S3'),
        ('Hive', 'Hive'),
        ('HDFS', 'HDFS'),
        # ('FTP', 'FTP'),
        # ('HTTP', 'HTTP'),
        # ('WebSocket', 'WebSocket'),
        # ('RESTAPI', 'RESTAPI'),
        # ('SOAP', 'SOAP'),
        ('CSV', 'CSV'),
        # ('Excel', 'Excel'),
        # ('File', 'File'),
    ]
    name = models.CharField(max_length=100)
    description = models.CharField(max_length=150)
    datasource_type = models.CharField(max_length=50,choices=DATASOURCE_TYPES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    connection_params = models.JSONField()  # Use JSONField for flexible connection parameters
    # Add any other fields you need for your data source
    # Metadata for mandatory and optional parameters
    CONNECTION_PARAMS_METADATA = {
        'Postgres': {
            'mandatory': ['host', 'port', 'username', 'password', 'database','query'],
            'optional': [],
        },
        'Mysql': {
            'mandatory': ['host', 'port', 'username', 'password', 'database','query'],
            'optional': [],
        },
        'Kafka': {
            'mandatory': ['brokers', 'topic'],
            'optional': ['group_id','schema_registry_url'],
        },
        'RabbitMQ': {
            'mandatory': ['host', 'port', 'username', 'password', 'queue'],
            'optional': [],
        },
        'S3': {
            'mandatory': ['bucket_name', 'access_key', 'secret_key'],
            'optional': ['region'],
        },
        'Hive': {
            'mandatory': ['host', 'port', 'username', 'password', 'database'],
            'optional': [],
        },
        'HDFS': {
            'mandatory': ['host', 'port', 'username', 'path'],
            'optional': [],
        },
        'CSV': {
            'mandatory': ['file_path'],
            'optional': ['delimiter', 'encoding'],
        },
    }

    def get_connection_params_metadata(self):
        """
        Returns the metadata for mandatory and optional parameters
        based on the datasource_type.
        """
        return self.CONNECTION_PARAMS_METADATA.get(self.datasource_type, {'mandatory': [], 'optional': []})

    # def clean(self):
    #     """
    #     Custom validation to ensure all mandatory parameters are provided.
    #     """
    #     metadata = self.get_connection_params_metadata()
    #     mandatory_params = metadata['mandatory']
    #     print('connection_params:', self.connection_params)
    #     # Check if all mandatory parameters are present in connection_params
    #     missing_params = [param for param in mandatory_params if param not in self.connection_params]
    #     if missing_params:
    #         raise ValidationError(f"Missing mandatory connection parameters: {', '.join(missing_params)}")

    def get_default_connection_params(self):
        """
        Returns default connection parameters based on the datasource_type.
        """
        defaults = {
            'Postgres': {
                'host': 'localhost',
                'port': 5432,
                'username': '',
                'password': '',
                'database': '',
                'query': '',
            },
            'Mysql': {
                'host': 'localhost',
                'port': 3306,
                'username': '',
                'password': '',
                'database': '',
                'query': '',
            },
            'Kafka': {
                'brokers': ['localhost:9092'],
                'topic': '',
                'group_id': '',
            },
            'RabbitMQ': {
                'host': 'localhost',
                'port': 5672,
                'username': '',
                'password': '',
                'queue': '',
            },
            'S3': {
                'bucket_name': '',
                'access_key': '',
                'secret_key': '',
                'region': '',
            },
            'Hive': {
                'host': 'localhost',
                'port': 10000,
                'username': '',
                'password': '',
                'database': '',
            },
            'HDFS': {
                'host': 'localhost',
                'port': 8020,
                'username': '',
                'path': '',
            },
            'CSV': {
                'file_path': '',
                'delimiter': ',',
                'encoding': 'utf-8',
            },
        }
        return defaults.get(self.datasource_type, {})
    def save(self, *args, **kwargs):
        # Set default connection_params based on datasource_type
        if not self.connection_params:
            self.connection_params = self.get_default_connection_params()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name
# class DataSourceConfig(models.Model):

