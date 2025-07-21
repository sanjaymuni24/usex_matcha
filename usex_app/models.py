from django.db import models
from django.core.exceptions import ValidationError
import re
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
    name = models.CharField(max_length=100,unique=True, help_text="Name of the data source.")
    description = models.CharField(max_length=150)
    datasource_type = models.CharField(max_length=50,choices=DATASOURCE_TYPES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    connection_params = models.JSONField()  # Use JSONField for flexible connection parameters
    skip_campaign_processing = models.BooleanField(default=False, help_text="Skip campaign processing for this datasource")
    enricher_params= models.JSONField(default=dict, blank=True, null=True, help_text="Parameters for enrichment processing")
    internal_name = models.CharField(max_length=100, unique=True, help_text="Internal name for the data source, used for Redis keys and other internal references.")
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
        
        if not self.internal_name:
            self.internal_name = f"{self.name.replace(' ', '_').lower()}"
        
        super().save(*args, **kwargs)
    def clean(self):
        """
        Custom validation for the `name` field.
        """
        # Validate name does not contain underscores or special characters
        if not re.match(r'^[A-Z][a-z]*( [A-Z][a-z]*)*$', self.name):
            raise ValidationError(
                "Name must not contain underscores or special characters, "
                "each word must start with a capital letter, and it must not exceed 100 characters."
            )

        # Ensure name does not exceed 100 characters
        if len(self.name) > 100:
            raise ValidationError("Name must not exceed 100 characters.")

        super().clean()

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
    aggregation_schema = models.JSONField(default=dict, blank=True, null=True)  # JSON field to store the aggregation schema
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    rejection_fields = models.JSONField(default=list, blank=True, null=True)
    enrichment_rejection_fields = models.JSONField(default=list, blank=True, null=True)
    
    enrichment_schema=models.JSONField(default=dict, blank=True, null=True)
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
    schema=models.JSONField(default=dict, blank=True, null=True)
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



class Enums(models.Model):
    """
    Model representing an enum set with options.
    """
    name = models.CharField(max_length=100, unique=True, help_text="Name of the enum set.")
    options = models.JSONField(default=dict, help_text="Options for the enum set as key-value pairs.")
    datatype= models.CharField(max_length=50, help_text="Data type of the enum values (e.g., string, integer).",default="str")
    enum_set_id = models.CharField(max_length=100, unique=True, help_text="Unique ID for the enum set.",default=None, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    def save(self, *args, **kwargs):
        # Generate enum_set_id from name
        if not self.enum_set_id:
            self.enum_set_id = self.name.lower().replace(" ", "_")
        super().save(*args, **kwargs)
    def __str__(self):
        return self.name
class Templates(models.Model):
    """
    Model representing condition templates for user-defined rules.
    """
    name = models.CharField(max_length=100,  help_text="Name of the condition template.")
    description = models.TextField(blank=True, help_text="Description of the condition template.")
    display_text = models.CharField(max_length=1000, help_text="Text to display in the UI for this condition template.",default="")
    template_expression=models.CharField(max_length=1000, help_text="Template expression for the condition, using placeholders for fields.",default="")
    datasource = models.ForeignKey(
        DataSource,
        on_delete=models.CASCADE,
        related_name='templates',
        help_text="The DataSource associated with this template."
    )
    selection_schema = models.JSONField(default=dict, blank=True, null=True, help_text="Schema for the selection fields in the template.")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    class Meta:
        unique_together = ('name', 'datasource')  # Ensure unique combination of name and datasource
        verbose_name = "Template"
        verbose_name_plural = "Templates"

    def __str__(self):
        return self.name
    # def clean(self):
    #     """
    #     Custom validation for the template expression.
    #     """
    #     tokens = self.template_expression.split(' ')
    #     stack = []

    #     for token in tokens:
    #         if token.startswith('$feed.') or token.startswith('$profile.') or token.startswith('$input.'):
    #             # Validate schema fields and inputs
    #             datatype = self.get_datatype(token)
    #             if not datatype:
    #                 raise ValidationError(f"Invalid field or input: {token}")
    #             stack.append(datatype)
    #         elif token.startswith('$enum.') or token.startswith('$operator.'):
    #             # Validate enums and operators
    #             operator = self.get_operator(token)
    #             if not operator:
    #                 raise ValidationError(f"Invalid operator or enum: {token}")

    #             required_operands = operator['operands']
    #             if len(stack) < required_operands:
    #                 raise ValidationError(f"Operator {operator['name']} requires {required_operands} operands.")

    #             operands = stack[-required_operands:]
    #             if not self.validate_operator_operands(operator, operands):
    #                 raise ValidationError(f"Invalid operands for operator {operator['name']}.")

    #             # Push the result datatype back to the stack
    #             stack = stack[:-required_operands]
    #             stack.append(operator['result_datatype'])
    #         else:
    #             raise ValidationError(f"Invalid token in expression: {token}")

    #     if len(stack) != 1:
    #         raise ValidationError("Invalid template expression. Please check the syntax.")

    # def get_datatype(self, token):
    #     """
    #     Fetch the datatype for a schema field or input.
    #     """
    #     if token.startswith('$feed.'):
    #         field_name = token.split('.')[1]
    #         field = self.datasource.schema.enrichment_schema.get(field_name)
    #         return field.get('datatype') if field else None
    #     elif token.startswith('$selection'):
    #         get_selection_datatype = self.get_selection_datatype(token)
    #         return get_selection_datatype if get_selection_datatype else None
    #         return input_field.datatype if input_field else None
    #     elif token.startswith('$profile.'):
    #         internal_name = token.split('.')[1]
    #         field= token.split('.')[2]
    #         datastore = DataStore.objects.filter(internal_name=internal_name).first()
    #         if datastore:
    #             field = datastore.schema.get(internal_name)
    #             return field.get('datatype') if field else None
    #         return input_field.datatype if input_field else None
        
    #     return None
    # def get_selection_datatype(self, token):
    #     for selection in self.selection_schema.keys():
    #         if self.selection_schema[selection]['unique_id'] == token:
    #             value= self.selection_schema[selection]['value']
    #             if value.split('.')[0]=='$enum':
    #                 enum_name = value.split('.')[1]
    #                 enum = Enums.objects.filter(enum_set_id=enum_name).first()
    #                 if enum:
    #                     return enum.datatype
    #             elif value.split('.')[0]=='$input':
    #                 input_name = value.split('.')[1]
    #                 return (input_name.lower)
                
                

            
    #     return None
    
class Operators(models.Model):
    """
    Model representing an operator with its category, description, and supported data types.
    """


    name = models.CharField(max_length=50, unique=True, help_text="Name of the operator (e.g., Equal, Not Equal).")
    options = models.JSONField(default=dict, help_text="Options for the enum set as key-value pairs.")
    operator_set_id = models.CharField(max_length=100, unique=True, help_text="Unique ID for the enum set.",default=None, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    def save(self, *args, **kwargs):
        # Generate enum_set_id from name
        if not self.operator_set_id:
            self.operator_set_id = self.name.lower().replace(" ", "_")
        super().save(*args, **kwargs)
    def __str__(self):
        return self.name
class Links(models.Model):
    """
    Model representing a relationship between two DataStores using columns from their schemas.
    """
    source_datastore = models.ForeignKey(
        DataStore,
        on_delete=models.CASCADE,
        related_name='source_links',
        help_text="The source DataStore in the relationship."
    )
    target_datastore = models.ForeignKey(
        DataStore,
        on_delete=models.CASCADE,
        related_name='target_links',
        help_text="The target DataStore in the relationship."
    )
    source_column = models.CharField(
        max_length=100,
        help_text="Column from the source DataStore schema used in the relationship."
    )
    target_column = models.CharField(
        max_length=100,
        help_text="Column from the target DataStore schema used in the relationship."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('source_datastore', 'target_datastore', 'source_column', 'target_column')
        verbose_name = "Link"
        verbose_name_plural = "Links"

    def __str__(self):
        return f"Link from {self.source_datastore.name}.{self.source_column} to {self.target_datastore.name}.{self.target_column}"
class CampaignProject(models.Model):
    """
    Model representing a project for campaigns.
    """
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
class Campaign(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    DataSource = models.ForeignKey(
        DataSource,
        on_delete=models.CASCADE,
        related_name='campaigns',
        help_text="The DataSource associated with this campaign."
    )
    project = models.ForeignKey(
        CampaignProject,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='campaigns',
        help_text="The project this campaign belongs to."
    )
    datastore = models.ForeignKey(
        DataStore,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='campaigns',
        help_text="The DataStore where campaign data will be stored.")
    profile_template_expression = models.CharField(
        max_length=3000,
        help_text="Template expression for the campaign profile, using placeholders for fields.",
        default=""
    )
    event_template_expression = models.CharField(
        max_length=3000,
        help_text="Template expression for the campaign event, using placeholders for fields.",
        default=""
    )
    profile_filter = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Filter conditions for the campaign profile, defining which profiles to include."
    )
    event_filter = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Filter conditions for the campaign event, defining which events to include."
    )
    

    actions = models.ManyToManyField(
        'Action',
        related_name='campaigns',
        blank=True,
        help_text="Actions associated with this campaign."
    )
    contact_policy = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Contact policy for the campaign, defining how and when to contact profiles."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    version = models.CharField(
        max_length=50,
        default='1.0',
        help_text="Version of the campaign, used for tracking changes."
    )
    status = models.CharField(
        max_length=20,
        choices=[
            ('draft', 'Draft'),
            ('active', 'Active'),
            ('archived', 'Archived')
        ],
        default='draft',
        help_text="Status of the campaign."
    )

    class Meta:
        unique_together = ('name', 'DataSource','project')  # Ensure unique combination of name, DataSource, and project
    
    def __str__(self):
        return self.name
class Action(models.Model):
    """
    Model representing an action in a campaign.
    """
    name = models.CharField(max_length=255)
    identifier = models.CharField(
        max_length=50,
        help_text="Unique identifier for the action."
    )
    profile_attributes= models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Attributes for the profile associated with this action."
    )
    feed_attributes = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        help_text="Attributes for the event associated with this action."
    )
    endpoint = models.ManyToManyField(
        'DataSink',
        related_name='actions',
        blank=True,
        help_text="Data sinks associated with this action."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        unique_together = ('name', 'identifier')  # Ensure unique combination of name and identifier
        verbose_name = "Action"
        verbose_name_plural = "Actions"
class DataSink(models.Model):
    """
    Model representing an sink for events in a campaign.
    """
    SINK_TYPES = [
        ('Postgres', 'postgres'),
        ('Mysql', 'Mysql'),
        ('Kafka', 'Kafka'),
        ('REST API', 'REST API')
        
    ]

    name = models.CharField(
        max_length=100,
        help_text="Name of the sink."
    )
    description =models.CharField(max_length=150,null=True, blank=True, help_text="Description of the sink.")
    datasink_type = models.CharField(
        max_length=50,
        choices=SINK_TYPES,
        null=True,
        help_text="Type of the sink (e.g., Database, Kafka, API, PostRequest)."
    )
    connection_params = models.JSONField(
        help_text="Connection parameters for the sink (e.g., host, port, credentials)."
    )
    enabled = models.BooleanField(
        default=False,
        help_text="Whether the sink is enabled for use."
    )
    output_schema = models.JSONField(default=dict, blank=True, null=True, help_text="Schema for the output data to be sent to the sink.")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    main_fields= models.JSONField(default=list, blank=True, null=True, help_text="Main fields for the sink schema.")
    CONNECTION_PARAMS_METADATA = {
        'Postgres': {
            'mandatory': ['host', 'port', 'username', 'password', 'database','table_name'],
            'optional': [],
        },
        'Mysql': {
            'mandatory': ['host', 'port', 'username', 'password', 'database','table_name'],
            'optional': [],
        },
        'Kafka': {
            'mandatory': ['brokers', 'topic'],
            'optional': ['group_id','schema_registry_url'],
        },
        'Hive': {
            'mandatory': ['host', 'port', 'username', 'password', 'database'],
            'optional': [],
        },
    }
    def get_connection_params_metadata(self):
        """
        Returns the metadata for mandatory and optional parameters
        based on the datasource_type.
        """
        return self.CONNECTION_PARAMS_METADATA.get(self.datasink_type, {'mandatory': [], 'optional': []})

    def get_default_connection_params(self):
        """
        Returns default connection parameters based on the sink type.
        """
        defaults = {
            'Postgres': {
                'host': 'localhost',
                'port': 5432,
                'username': '',
                'password': '',
                'database': '',
                'table_name': '',
            },
            'Mysql': {
                'host': 'localhost',
                'port': 3306,
                'username': '',
                'password': '',
                'database': '',
                'table_name': '',
            },
            'Kafka': {
                'brokers': ['localhost:9092'],
                'topic': '',
                'group_id': '',
                'schema_registry_url': '',
            },
            'API': {
                'base_url': '',
                'headers': {},
                'timeout': 30,
            },
            'Hive': {
                'host': 'localhost',
                'port': 10000,
                'username': '',
                'password': '',
                'database': '',
            },
        }
        return defaults.get(self.datasink_type, {})

    def save(self, *args, **kwargs):
        # Set default connection_params based on sink_type
        if not self.connection_params:
            self.connection_params = self.get_default_connection_params()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} ({self.datasink_type})"


    
from django.db import models
from django.utils.timezone import now


class SystemHealth(models.Model):
    """
    Model to store system health check data for running feed datasources.
    """
    datasource_name = models.CharField(
        max_length=100,
        help_text="Name of the datasource that registered itself."
    )
    ip_address = models.GenericIPAddressField(
        help_text="IP address of the instance running the datasource."
    )
    registered_instance_count = models.PositiveIntegerField(
        default=1,
        help_text="Number of instances registered for this datasource."
    )
    cpu_utilization = models.FloatField(
        default=0.0,
        help_text="CPU utilization percentage of the instance."
    )
    memory_utilization = models.FloatField(
        default=0.0,
        help_text="Memory utilization percentage of the instance."
    )
    last_updated = models.DateTimeField(
        auto_now=True,
        help_text="Timestamp of the last health check update."
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        help_text="Timestamp when the datasource was first registered."
    )

    class Meta:
        unique_together = ('datasource_name', 'ip_address')  # Ensure unique combination of datasource and IP address
        verbose_name = "System Health"
        verbose_name_plural = "System Health Records"

    def __str__(self):
        return f"{self.datasource_name} ({self.ip_address})"

    @classmethod
    def register_or_update(cls, datasource_name, ip_address, cpu_utilization, memory_utilization):
        """
        Register a new instance or update an existing one for the given datasource and IP address.

        Args:
            datasource_name (str): Name of the datasource.
            ip_address (str): IP address of the instance.
            cpu_utilization (float): CPU utilization percentage.
            memory_utilization (float): Memory utilization percentage.
        """
        instance, created = cls.objects.get_or_create(
            datasource_name=datasource_name,
            ip_address=ip_address,
            defaults={
                'cpu_utilization': cpu_utilization,
                'memory_utilization': memory_utilization,
                'registered_instance_count': 1
            }
        )
        if not created:
            # Update the existing record
            instance.cpu_utilization = cpu_utilization
            instance.memory_utilization = memory_utilization
            instance.registered_instance_count += 1
            instance.last_updated = now()
            instance.save()