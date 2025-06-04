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
            'optional': ['delimiter', 'encoding','has_header'],
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
                'schema_registry_url': '',
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
                'has_header': True,  # Assuming CSV has a header by default
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
class DataSourceSchema(models.Model):
    """
    Model representing the schema for a data source.
    """
    datasource = models.OneToOneField(DataSource, on_delete=models.CASCADE, related_name='schema')
    input_schema = models.JSONField()  # JSON field to store the input schema
    parsing_schema = models.JSONField()  # JSON field to store the pre-enrichment schema
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    rejection_fields = models.JSONField()
    enrichment_schema=models.JSONField()
    def __str__(self):
        return f"Schema for {self.datasource.name}"
class DataStore(models.Model):
    """
    Model representing a data store for storing processed data.
    """
    name = models.CharField(max_length=100, unique=True)
    description = models.CharField(max_length=150)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    key= models.CharField(max_length=100, unique=True)
    internal_name = models.CharField(max_length=100, unique=True, null=True, blank=True)
    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        if not self.internal_name:
            self.internal_name = f"{self.name.replace(' ', '_').lower()}_{self.key}"
        super().save(*args, **kwargs)

class Relationship(models.Model):
    """
    Model representing a relationship between a DataSource and a DataStore.
    """
    datasource = models.ForeignKey(
        DataSource,
        on_delete=models.CASCADE,
        related_name='relationships',
        help_text="The DataSource associated with this relationship."
    )
    datastore = models.ForeignKey(
        DataStore,
        on_delete=models.CASCADE,
        related_name='relationships',
        help_text="The DataStore associated with this relationship."
    )
    datasource_key = models.CharField(
        max_length=100,
        help_text="Key from the DataSource used to access the DataStore.",
        default="default_key"

    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('datasource', 'datastore')  # Ensure unique pairing of DataSource and DataStore

    def __str__(self):
        return f"Relationship between {self.datasource.name} and {self.datastore.name}"


