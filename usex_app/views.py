from django.shortcuts import render
from .models import DataSource,DataSourceSchema,DataStore,Relationship,Enums,Templates,Operators,Links,Campaign,DataSink,CampaignProject,Action
from .forms import DataSourceForm,DataStoreForm,DataSinkForm
import  json
from django.shortcuts import redirect,get_object_or_404
from django.http import JsonResponse
from .utility.data_source_connection import connect_to_data_source,query_dataset,connect_to_data_sink,generate_enricher_params
from .utility.operators import ColumnOperatorsWrapper, FormulaInterpreter
import traceback
import redis
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException,Consumer,TopicPartition
import time
import traceback
from datetime import datetime
from django.conf import settings
# Create your views here.
def home(request):
    """
    Render the homepage of the application.
    """
    return render(request, 'usex_app/home.html')
def about(request):
    """
    Render the about page of the application.
    """
    return render(request, 'usex_app/about.html')
def contact(request):
    """
    Render the contact page of the application.
    """
    return render(request, 'usex_app/contact.html')
def services(request):
    """
    Render the services page of the application.
    """
    return render(request, 'usex_app/services.html')
def data_source_list(request):
    """
    List all data sources.
    """
    data_sources = DataSource.objects.all()
    return render(request, 'usex_app/data_source_list.html', {'data_sources': data_sources})
def data_source_create(request):
    """
    Create a new data source.
    """
    if request.method == 'POST':
        form = DataSourceForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('data_source_list')
    else:
        form = DataSourceForm()
    return render(request, 'usex_app/data_source_form.html', {'form': form})

def delete_datasource(request, datasource_id):
    datasource = get_object_or_404(DataSource, id=datasource_id)
    datasource.delete()
    return redirect('datasource_list_create')

def edit_datasource(request):
    
    if request.method == 'POST':
        print('request.POST:', request.POST)
        datasource_id = request.POST.get('datasource_id')
        datasource_type = request.POST.get('datasource_type')
        datasource = get_object_or_404(DataSource, id=datasource_id)
        form = DataSourceForm(request.POST, instance=datasource, datasource_type=datasource_type)
        if form.is_valid():
            # Extract connection_params from the dynamic fields
            connection_params = {}

            for key, value in form.cleaned_data.items():
                print('key:', key)
                print('value:', value)
                if key.startswith('connection_params_'):
                    param_name = key.replace('connection_params_', '')
                    connection_params[param_name] = value

            # Update the DataSource instance
            datasource = form.save(commit=False)
            datasource.connection_params = connection_params
            print('connection_params:', connection_params)

            # Check the connection
            try:
                print(datasource)
                connection = connect_to_data_source(datasource)
                # If the connection is successful, save the data source
                datasource.save()
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({'success': True})
                return redirect('datasource_list')
            except Exception as e:
                # If the connection fails, add an error to the form
                form.add_error(None, f"Connection failed: {str(e)}")
                

        # If the form is invalid, return errors for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            errors = [str(error) for error in form.non_field_errors()]
            for field, field_errors in form.errors.items():
                errors.extend([f"{field}: {error}" for error in field_errors])
            return JsonResponse({'success': False, 'errors': errors})

    else:
        form = DataSourceForm(instance=datasource)
    return render(request, 'usex_app/edit_datasource.html', {'form': form, 'datasource': datasource})
def data_source_detail(request, pk):
    """
    View the details of a data source.
    """
    data_source = DataSource.objects.get(pk=pk)
    return render(request, 'usex_app/data_source_detail.html', {'data_source': data_source})
def datasource_list_create(request):
    if request.method == 'POST':
        # Handle DataSourceForm submission
        datasource_type = request.POST.get('datasource_type')
        datasource_form = DataSourceForm(request.POST, datasource_type=datasource_type)
        if datasource_form.is_valid():
            # Extract connection_params from the dynamic fields
            connection_params = {}
            for key, value in datasource_form.cleaned_data.items():
                if key.startswith('connection_params_'):
                    param_name = key.replace('connection_params_', '')
                    connection_params[param_name] = value

            # Create a temporary DataSource instance to validate the connection
            datasource = datasource_form.save(commit=False)
            datasource.connection_params = connection_params

            # Check the connection
            try:
                connection = connect_to_data_source(datasource)
                # If the connection is successful, save the data source
                datasource.save()
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({'success': True})
                return redirect('datasource_list_create')
            except Exception as e:
                # If the connection fails, add an error to the form
                datasource_form.add_error(None, f"Connection failed: {str(e)}")

        # If the form is invalid, return errors for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            errors = [str(error) for error in datasource_form.non_field_errors()]
            for field, field_errors in datasource_form.errors.items():
                errors.extend([f"{field}: {error}" for error in field_errors])
            return JsonResponse({'success': False, 'errors': errors})

        # Handle DataStoreForm submission
        datastore_form = DataStoreForm(request.POST)
        if datastore_form.is_valid():
            datastore_form.save()
            return redirect('datasource_list_create')

    else:
        # Initialize both forms for GET requests
        datasource_form = DataSourceForm()
        datastore_form = DataStoreForm()
        datasink_form = DataSinkForm()

    # Fetch all datasources and datastores
    datasources = DataSource.objects.all()
    datastores = DataStore.objects.all()
    datasinks = DataSink.objects.all()

    # Prepare metadata for connection parameters
    connection_params_metadata = json.dumps(DataSource.CONNECTION_PARAMS_METADATA)
    connection_params_values = {ds.id: ds.connection_params for ds in datasources}

    return render(request, 'usex_app/datasource.html', {
        'datasources': datasources,
        'datastores': datastores,
        'datasinks': datasinks,
        'datasource_form': datasource_form,
        'datastore_form': datastore_form,
        'datasink_form': datasink_form,
        'connection_params_metadata': connection_params_metadata,
        'connection_params_values': json.dumps(connection_params_values),
        'datasink_params_metadata': json.dumps(DataSink.CONNECTION_PARAMS_METADATA),
        'datasink_params_values': {ds.id: ds.connection_params for ds in datasinks},
    })
def datasource_connection_check(request, pk):
    """
    Check the connection to a data source.
    """
    data_source = DataSource.objects.get(pk=pk)
    # Implement your connection check logic here
    # For example, you can use the connection_params to establish a connection
    # and return success or failure message.
    try:
        connection = connect_to_data_source(data_source)
        # If the connection is successful, you can return a success message
        connection_status = "Connection successful"
    except Exception as e:
        # If there is an error, you can return a failure message
        connection_status = f"Connection failed: {str(e)}"
    
    return JSONResponse({
        'connection_status': connection_status,
        'data_source': data_source.name,
    })
def enrichments(request):
    """
    Render the enrichments page of the application.
    """
    return render(request, 'usex_app/parser.html')
def query_data(request):
    """
    Query data from Postgres or Kafka based on the data source type and return the results.
    """
    if request.method == 'GET':
        datasource_id = request.GET.get('datasource_id')  # Get the data source ID from the request
        datasource = DataSource.objects.get(id=datasource_id)  # Fetch the data source from the database

        result_dict=query_dataset(datasource)  # Call the query_dataset function to get the results
        return JsonResponse(result_dict)

    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def parser_view(request, datasource_id):
    datasource = get_object_or_404(DataSource, id=datasource_id)
    return render(request, 'usex_app/parser.html', {'datasource': datasource})
def enrichment_view(request, datasource_id):
    datasource = get_object_or_404(DataSource, id=datasource_id)
    return render(request, 'usex_app/enrichments.html', {'datasource': datasource})
def fetch_datasource_schema(request, datasource_id):
    if request.method == 'GET':
        try:
            # Replace with logic to fetch the datasource by ID
            datasource = DataSource.objects.get(id=datasource_id) # Implement this function
            try:
                stored_schema=datasource.schema.input_schema or {}
            except:
                stored_shema={}
            try:
                parsing_schema=datasource.schema.parsing_schema or {}
            except:
                parsing_schema={}
            try:
                enrichment_schema=datasource.schema.enrichment_schema or {}
            except:
                enrichment_schema={}
            result={'success': True, "stored_schema":stored_schema,'parsing_schema':parsing_schema, 'enrichment_schema':enrichment_schema}
            return JsonResponse(result)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def fetch_query_dataset(request, datasource_id):
    if request.method == 'GET':
        try:
            # Replace with logic to fetch the datasource by ID
            datasource = DataSource.objects.get(id=datasource_id) # Implement this function
            result = query_dataset(datasource)

            if result['success']:
                # Extract column names and the first record
                result['columns'] = [col for col in result['results'][0].keys()] if result['results'] else []
                result['first_record'] = [val for val in result['results'][0].values()] if result['results'] else []
                
                result['column_datatypes'] = [result['schema'][key] for key in result['results'][0].keys()] if result['schema'] else []
                # datatype_columns = result['schema']
                
                return JsonResponse(result)
            else:
                return JsonResponse({'success': False, 'error': result['error']})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def update_schema(request, datasource_id):
    if request.method == 'POST':
        try:
            # Fetch the datasource
            datasource = DataSource.objects.get(id=datasource_id)
            new_schema = json.loads(request.body).get('schema')

            # Update the schema in the backend
            if hasattr(datasource, 'schema'):
                datasource.schema.input_schema = new_schema
                datasource.schema.save()
            else:
                DataSourceSchema.objects.create(
                    datasource=datasource,
                    input_schema=new_schema,
                    parsing_schema={}
                )

            return JsonResponse({'success': True, 'message': 'Schema updated successfully.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_operations(request):
    return JsonResponse(ColumnOperatorsWrapper.get_operations())
def formula_interpreter_api(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            formula = data.get('field_expression')
            column_values = data.get('column_values')
            print("Received formula:", formula)
            print("Received column values:", column_values)
            # column_datatype = data.get('column_datatype')
            result_value,result_datatype = FormulaInterpreter.evaluate_formula(formula, column_values)
            print("Result of formula evaluation:", result_value ,result_datatype)

            return JsonResponse({'success': True, 'result': {
                    'value': result_value,
                    'datatype': result_datatype
                }})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def aggregation_interpreter_api(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            aggregation_type = data.get('aggregation_type')
            field_name = data.get('field_name')
            group_by_fields = data.get('group_by_fields')
            field_expression = data.get('field_expression')
            column_values = data.get('column_values')

            print("Received aggregation type:", aggregation_type)
            print("Received field name:", field_name)
            print("Received group_by_fields:", group_by_fields)
            print("Received field_expression:", field_expression)
            print("Received column values:", column_values)

            # Perform the aggregation based on the type
            if aggregation_type == 'sum':
                result_value = len(column_values)
            elif aggregation_type == 'count':
                result_value = len(column_values)
            elif aggregation_type == 'average':
                result_value = sum(column_values) / len(column_values) if column_values else 0
            else:
                return JsonResponse({'success': False, 'error': 'Invalid aggregation type.'})
            result_value,result_datatype = FormulaInterpreter.evaluate_formula(field_expression, column_values)
            print("Result of aggregation:", result_value)

            return JsonResponse({'success': True, 'result': {
                    'value': result_value,
                    'datatype': type(result_value).__name__  # Get the datatype of the result
                }})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def update_parser_schema(datasource_id, field_name, formula, result, datatype):
    """
    Update the parsing_schema of the DataSourceSchema model.

    Args:
        datasource_id (int): ID of the DataSource.
        field_name (str): Name of the calculated field.
        formula (str): Formula used for the calculated field.
        result (Any): Result of the formula evaluation.
        datatype (str): Datatype of the result.

    Returns:
        bool: True if the schema was updated successfully, False otherwise.
    """
    try:
        # Fetch the DataSourceSchema associated with the DataSource
        datasource = DataSource.objects.get(id=datasource_id)
        schema = datasource.schema

        if schema:
            # Update or add the field in parsing_schema
            parsing_schema = schema.parsing_schema or {}
            parsing_schema[field_name] = {
                'formula': formula,
                'result': result,
                'datatype': datatype
            }
            schema.parsing_schema = parsing_schema
            schema.save()
            return True
        else:
            print(f"No schema found for DataSource ID: {datasource_id}")
            return False
    except Exception as e:
        print(f"Error updating parsing_schema: {e}")
        return False
def update_parser_schema(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')
            formula = data.get('formula')
            result = data.get('result')
            datatype = data.get('datatype')

            # Update the parsing_schema
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema:
                # Update or add the field in parsing_schema
                parsing_schema = schema.parsing_schema or {}
                parsing_schema[field_name] = {
                    'formula': formula,
                    'result': result,
                    'datatype': datatype
                }
                schema.parsing_schema = parsing_schema
                schema.save()
                return JsonResponse({'success': True, 'message': 'Pre-enrichment schema updated successfully.'})
            else:
                print(f"No schema found for DataSource ID: {datasource_id}")
                return JsonResponse({'success': False, 'error': 'Failed to update pre-enrichment schema.'})
            
                
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def update_enrichment_schema(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')
            formula = data.get('formula')
            result = data.get('result')
            datatype = data.get('datatype')
            enrichment_name=field_name.replace(' ','_').lower()

            # Update the parsing_schema
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema:
                # Update or add the field in parsing_schema
                enrichment_schema = schema.enrichment_schema or {}
                enrichment_schema[enrichment_name] = {
                    'formula': formula,
                    'result': result,
                    'datatype': datatype,
                    'field_name': field_name
                }
                schema.enrichment_schema = enrichment_schema
                schema.save()
                return JsonResponse({'success': True, 'message': 'Pre-enrichment schema updated successfully.'})
            else:
                print(f"No schema found for DataSource ID: {datasource_id}")
                return JsonResponse({'success': False, 'error': 'Failed to update pre-enrichment schema.'})
            
                
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def delete_parser_field(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')

            # Fetch the DataSourceSchema associated with the DataSource
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema and field_name in schema.parsing_schema:
                # Remove the field from parsing_schema
                del schema.parsing_schema[field_name]
                schema.save()
                return JsonResponse({'success': True, 'message': f'Field "{field_name}" deleted successfully.'})
            else:
                return JsonResponse({'success': False, 'error': f'Field "{field_name}" not found in parsing_schema.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def delete_enrichment_field(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')

            # Fetch the DataSourceSchema associated with the DataSource
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema and field_name in schema.enrichment_schema:
                # Remove the field from enrichment_schema
                del schema.enrichment_schema[field_name]

                # Remove the field from enrichment_rejection_fields if it exists
                enrichment_rejection_fields = schema.enrichment_rejection_fields or []
                if field_name in enrichment_rejection_fields:
                    enrichment_rejection_fields.remove(field_name)
                    schema.enrichment_rejection_fields = enrichment_rejection_fields

                # Save the updated schema
                schema.save()

                return JsonResponse({'success': True, 'message': f'Field "{field_name}" deleted successfully.'})
            else:
                return JsonResponse({'success': False, 'error': f'Field "{field_name}" not found in enrichment_schema.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def template_view(request):
    datasources = DataSource.objects.filter(schema__enrichment_schema__isnull=False)
    enums = Enums.objects.all()
    operators= Operators.objects.all()
    return render(request, 'usex_app/templates.html', {'datasources': datasources, 'enums': enums, 'operators': operators})

def datastore_view(request):
    datastores = DataStore.objects.all()
    return render(request, 'usex_app/datasource.html', {'datastores': datastores})

def create_datastore(request):
    if request.method == 'POST':
        datastore_form = DataStoreForm(request.POST)
        if datastore_form.is_valid():
            datastore_form.save()
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': True})
            return redirect('datasource_list_create')  # Redirect to the main page
        else:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                errors = [str(error) for error in datastore_form.non_field_errors()]
                for field, field_errors in datastore_form.errors.items():
                    errors.extend([f"{field}: {error}" for error in field_errors])
                return JsonResponse({'success': False, 'errors': errors})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def delete_datastore(request, datastore_id):
    datastore = get_object_or_404(DataStore, id=datastore_id)
    datastore.delete()
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse({'success': True})
    return redirect('datasource_list_create')
def edit_datastore(request):
    if request.method == 'POST':
        datastore_id = request.POST.get('datastore_id')
        datastore = DataStore.objects.get(pk=datastore_id)
        datastore_form = DataStoreForm(request.POST, instance=datastore)
        if datastore_form.is_valid():
            datastore_form.save()
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': True})
            return redirect('datasource_list_create')
        else:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                errors = [str(error) for error in datastore_form.non_field_errors()]
                for field, field_errors in datastore_form.errors.items():
                    errors.extend([f"{field}: {error}" for error in field_errors])
                return JsonResponse({'success': False, 'errors': errors})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def get_pre_enrichment_schema(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        if datasource.schema and datasource.schema.parsing_schema:
            fieldnames = list(datasource.schema.parsing_schema.keys())
            return JsonResponse({'success': True, 'fieldnames': fieldnames})
        return JsonResponse({'success': False, 'error': 'No parsing_schema available.'})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def get_relationships_datastore(request):
    datastore_id = request.GET.get('datastore_id')
    
    try:
        relationships = Relationship.objects.filter(datastore_id=datastore_id).select_related('datasource')
        relationship_data = [
            {
                'datasource_name': relationship.datasource.name,
                'fieldname': relationship.datasource_key,
                'datastore_name': relationship.datastore.name,
                'datastore_key': relationship.datastore.key,

            }
            for relationship in relationships
        ]
        return JsonResponse({'success': True, 'relationships': relationship_data})
    except Exception as e:
        print('Error fetching relationships:', e)
        raise e
        return JsonResponse({'success': False, 'error': str(e)})
def create_relationship(request):
    if request.method == 'POST':
        datastore_id = request.POST.get('datastore_id')
        datasource_id = request.POST.get('datasource_id')
        fieldname = request.POST.get('fieldname')

        try:
            datastore = DataStore.objects.get(pk=datastore_id)
            datasource = DataSource.objects.get(pk=datasource_id)

            # Create a new relationship
            relationship = Relationship.objects.create(
                datastore=datastore,
                datasource=datasource,
                datasource_key=fieldname
            )
            return JsonResponse({'success': True, 'message': 'Relationship created successfully.'})
        except DataStore.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataStore not found.'})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
    
def get_input_schema(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        rejection_fields = datasource.schema.rejection_fields if datasource.schema else []
        if datasource.schema and datasource.schema.input_schema:
            fieldnames = [field for field in list(datasource.schema.input_schema.keys()) if field not in rejection_fields]
            return JsonResponse({'success': True, 'fieldnames': fieldnames})
        return JsonResponse({'success': False, 'error': 'No input_schema available.'})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def get_rejection_schema(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        rejection_fields = datasource.schema.rejection_fields if datasource.schema else []
        return JsonResponse({'success': True, 'rejection_fields': rejection_fields})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def update_rejection_schema(request):
    if request.method == 'POST':
        data= json.loads(request.body)
        datasource_id = data.get('datasource_id')
        field_name = data.get('field_name')
        
        try:
            datasource = DataSource.objects.get(pk=datasource_id)
            if datasource.schema:
                rejection_fields = datasource.schema.rejection_fields or []
                
                print('rejection_fields:', rejection_fields)
                if field_name not in rejection_fields:
                    rejection_fields.append(field_name)
                    datasource.schema.rejection_fields = rejection_fields
                    datasource.schema.save()
                return JsonResponse({'success': True})
            return JsonResponse({'success': False, 'error': 'Schema not found.'})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def delete_rejection_field(request):
    if request.method == 'POST':
        print('request.POST:', request.POST)
        print('request.body:', request.body)
        data = json.loads(request.body)
        datasource_id = data.get('datasource_id')
        field_name = data.get('field_name')
        try:
            datasource = DataSource.objects.get(pk=datasource_id)
            if datasource.schema:
                rejection_fields = datasource.schema.rejection_fields or []
                if field_name in rejection_fields:
                    rejection_fields.remove(field_name)
                    datasource.schema.rejection_fields = rejection_fields
                    datasource.schema.save()
                return JsonResponse({'success': True})
            return JsonResponse({'success': False, 'error': 'Schema not found.'})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def get_enrichment_rejection_schema(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        enrichment_rejection_fields = datasource.schema.enrichment_rejection_fields if datasource.schema else []
        return JsonResponse({'success': True, 'enrichment_rejection_fields': enrichment_rejection_fields})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def update_enrichment_rejection_schema(request):
    if request.method == 'POST':
        data= json.loads(request.body)
        datasource_id = data.get('datasource_id')
        field_name = data.get('field_name')
        
        try:
            datasource = DataSource.objects.get(pk=datasource_id)
            if datasource.schema:
                enrichment_rejection_fields = datasource.schema.enrichment_rejection_fields or []
                
                print('enrichment_rejection_fields:', enrichment_rejection_fields)
                if field_name not in enrichment_rejection_fields:
                    enrichment_rejection_fields.append(field_name)
                    datasource.schema.enrichment_rejection_fields = enrichment_rejection_fields
                    datasource.schema.save()
                return JsonResponse({'success': True})
            return JsonResponse({'success': False, 'error': 'Schema not found.'})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def delete_enrichment_rejection_field(request):
    if request.method == 'POST':
        print('request.POST:', request.POST)
        print('request.body:', request.body)
        data = json.loads(request.body)
        datasource_id = data.get('datasource_id')
        field_name = data.get('field_name')
        try:
            datasource = DataSource.objects.get(pk=datasource_id)
            if datasource.schema:
                enrichment_rejection_fields = datasource.schema.enrichment_rejection_fields or []
                if field_name in enrichment_rejection_fields:
                    enrichment_rejection_fields.remove(field_name)
                    datasource.schema.enrichment_rejection_fields = enrichment_rejection_fields
                    datasource.schema.save()
                return JsonResponse({'success': True})
            return JsonResponse({'success': False, 'error': 'Schema not found.'})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_relationships(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        relationships = Relationship.objects.filter(datasource_id=datasource_id).select_related('datastore')
        print('relationships:', relationships)
        print(relationships[0].datastore.schema.items())
        related_datastore=[relationship.datastore for relationship in relationships]
        for relationship in relationships:
            links = Links.objects.filter(source_datastore=relationship.datastore) 
            for link in links:
                if link.target_datastore not in related_datastore:
                    related_datastore.append(link.target_datastore)
        print('related_datastore:', related_datastore)
        # Prepare the data to be returned
        relationship_data = [
            {
                'datastore_name': datastore.name,
                'datastore_internal_name': datastore.internal_name,
                'datastore_key': datastore.key,
                'columns': [
                    {'name': column_name, 'data': column_data}
                    for column_name, column_data in datastore.schema.items()
                ],
            }
            for datastore in related_datastore
        ]
        return JsonResponse({'success': True, 'relationships': relationship_data})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
def update_storeback_datastore(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            datasource_id = data.get('datasource_id')
            field_name = data.get('field_name')
            storeback_table = data.get('storeback_table')
            
            

            datasource = DataSource.objects.get(pk=datasource_id)

            datastore = DataStore.objects.get(internal_name=storeback_table)
            if not datastore and storeback_table!= '':
                return JsonResponse({'success': False, 'error': 'Datastore not found.'})
            if datasource.schema and field_name in datasource.schema.enrichment_schema:
                enrichment_schema = datasource.schema.enrichment_schema
                current_storeback_table=enrichment_schema[field_name].get('storeback_table', '')
                
                if storeback_table == '' and current_storeback_table != '':
                    current_datastore= DataStore.objects.get(internal_name=current_storeback_table)
                    if current_datastore:
                        datastore_schema = current_datastore.schema or {}
                        if field_name in datastore_schema:
                            # Remove the field from the datastore schema    
                            del datastore_schema[field_name]
                            current_datastore.schema = datastore_schema
                            current_datastore.save()
                        
                if datastore:
                    datastore_schema = datastore.schema or {}
                    datastore_schema[field_name] = {
                        'datatype': enrichment_schema[field_name]['datatype'],
                        'formula': enrichment_schema[field_name]['formula'],
                        'result': enrichment_schema[field_name]['result'],
                        'datasource_id': datasource_id,
                    }
                    datastore.schema = datastore_schema
                    datastore.save()
                
                enrichment_schema[field_name]['storeback_table'] = storeback_table
                datasource.schema.enrichment_schema = enrichment_schema
                datasource.schema.save()
                return JsonResponse({'success': True})
            return JsonResponse({'success': False, 'error': 'Field not found in enrichment schema.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
        
def get_enrichment_schema_rejection_fields(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        enrichment_rejection_fields = datasource.schema.enrichment_rejection_fields if datasource.schema else []
        if datasource.schema and datasource.schema.enrichment_schema:
            fieldnames = [field for field in list(datasource.schema.enrichment_schema.keys()) if field not in enrichment_rejection_fields]
            return JsonResponse({'success': True, 'fieldnames': fieldnames})
        return JsonResponse({'success': False, 'error': 'No input_schema available.'})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def get_skip_campaign_processing_state(request):
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(pk=datasource_id)
        skip_campaign_processing = datasource.skip_campaign_processing  # Assuming this field exists in the model
        return JsonResponse({'success': True, 'skip_campaign_processing': skip_campaign_processing})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'DataSource not found.'})
def update_skip_campaign_processing_state(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            datasource_id = data.get('datasource_id')
            skip_campaign_processing = data.get('skip_campaign_processing')

            datasource = DataSource.objects.get(pk=datasource_id)
            datasource.skip_campaign_processing = skip_campaign_processing  # Update the field
            datasource.save()

            return JsonResponse({'success': True})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def list_enums(request):
    enums = Enums.objects.all()
    return render(request, 'usex_app/enums.html', {'enums': enums})
def create_enum(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            name = data.get('name')
            options = data.get('options')

            enum = Enums.objects.create(name=name, options=options)
            return JsonResponse({'success': True, 'enum_id': enum.id})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def edit_enum(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            enum_id = data.get('id')
            name = data.get('name')
            options = data.get('options')

            enum = Enums.objects.get(id=enum_id)
            enum.name = name
            enum.options = options
            enum.save()
            return JsonResponse({'success': True})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})



def create_template(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            name = data.get('name')
            description = data.get('description', '')
            expression = data.get('expression')
            display_text = data.get('display_text', '')
            datasource_id = data.get('datasource_id')
            selections = data.get('selections', [])  # Get selections from the payload
            datasource = DataSource.objects.get(id=datasource_id)
            template = Templates.objects.create(
                name=name,
                description=description,
                display_text=display_text,
                template_expression=expression,
                datasource=datasource,
                selection_schema=selections
            )
            return JsonResponse({'success': True, 'template_id': template.id})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def get_template_by_id(request,template_id):
    try:
        template=Templates.objects.get(id=template_id)
        if not template:
            return JsonResponse({'success':False,'error': 'Template not found'})
        print(template)
        # Prepare the template data
        template_data = {
            'id': template.id,
            'name': template.name,
            'description': template.description,
            'template_expression': template.template_expression,
            'display_text': template.display_text,
            'datasource_id': template.datasource.id,
            'selections': template.selection_schema,  # Assuming selection_schema is stored as JSON
        }

        return JsonResponse({'success': True, 'template': template_data})
    except Templates.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Template not found'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})

def get_templates_for_datasource(request, datasource_id):
    try:
        datasource= DataSource.objects.get(id=datasource_id)
        if not datasource:
            return JsonResponse({'success': False, 'error': 'DataSource not found.'})
        print(datasource)
        templates = Templates.objects.filter(datasource=datasource)
        template_data = [
            {
                'id': template.id,
                'name': template.name,
                'description': template.description,
            }
            for template in templates
        ]
        
        return JsonResponse({'success': True, 'templates': template_data})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})

def get_links(request):
    source_datastore_id = request.GET.get('source_datastore_id')
    links = Links.objects.filter(source_datastore_id=source_datastore_id)
    links_data = [
        {
            'source_datastore_name': link.source_datastore.name,
            'target_datastore_name': link.target_datastore.name,
            'target_schema_field': link.target_column,
            'source_schema_field': link.source_column,
            'id': link.id,
        }
        for link in links
    ]
    return JsonResponse({'success': True, 'links': links_data})
def get_datastore_schema(request):
    datastore_id = request.GET.get('datastore_id')
    datastore = DataStore.objects.get(id=datastore_id)
    schema_fields = datastore.schema.keys() if datastore.schema else []
    return JsonResponse({'success': True, 'schema_fields': list(schema_fields)})
def create_link(request):
    if request.method == 'POST':
        source_datastore_id = request.POST.get('source_datastore_id')
        target_datastore_id = request.POST.get('target_datastore_id')
        target_schema_field = request.POST.get('target_schema_field')
        source_schema_field = request.POST.get('source_schema_field')
        
        try:
            source_datastore = DataStore.objects.get(id=source_datastore_id)
            target_datastore = DataStore.objects.get(id=target_datastore_id)

            link = Links.objects.create(
                source_datastore=source_datastore,
                target_datastore=target_datastore,
                source_column=source_schema_field,  # Assuming source column is the key
                target_column=target_schema_field
            )
            return JsonResponse({'success': True})
        except Exception as e:
            return JsonResponse({'success': False, 'errors': [str(e)]})

def update_template_by_id(request, template_id):
    if request.method == 'PUT':  # Use PUT for updates
        try:
            # Parse the incoming JSON payload
            data = json.loads(request.body)
            name = data.get('name')
            description = data.get('description', '')
            expression = data.get('expression')
            display_text = data.get('display_text', '')
            datasource_id = data.get('datasource_id')
            selections = data.get('selections', [])  # Get selections from the payload

            # Fetch the existing template
            template = Templates.objects.get(id=template_id)

            # Update the template fields
            template.name = name
            template.description = description
            template.template_expression = expression
            template.display_text = display_text
            template.datasource_id = datasource_id
            template.selection_schema = selections  # Update the selection schema

            # Save the updated template
            template.save()

            return JsonResponse({'success': True, 'template_id': template.id})
        except Templates.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Template not found'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def campaign_list_create(request):
    campaigns = Campaign.objects.all()
    datasources= DataSource.objects.filter(schema__enrichment_schema__isnull=False)
    projects = CampaignProject.objects.all()
    print('datasources:', datasources)
    return render(request, 'usex_app/campaigns.html', {'campaigns': campaigns,'datasources': datasources,'projects': projects})
def create_campaign(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            datasource_id = data.get('datasource_id')
            profile_template_expression = data.get('profile_template_expression')
            event_template_expression = data.get('event_template_expression')
            contact_policy = data.get('contact_policy')

            datasource = DataSource.objects.get(id=datasource_id)

            Campaign.objects.create(
                name=f"Campaign for {datasource.name}",
                description="Auto-generated campaign",
                datasource=datasource,
                profile_template_expression=profile_template_expression,
                event_template_expression=event_template_expression,
                contact_policy=contact_policy,
            )

            return JsonResponse({'success': True})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def create_campaign_view(request):
    # Filter datasources that have an enrichment schema
    datasources = DataSource.objects.filter(schema__enrichment_schema__isnull=False)

    days_list= ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return render(request, 'usex_app/edit_campaign.html', {'datasources': datasources,'days_list': days_list})
def edit_campaign(request, campaign_id):
    campaign = Campaign.objects.get(id=campaign_id)
    projects = CampaignProject.objects.all()
    days_list= ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    datasource= campaign.DataSource
    if not datasource:
        return JsonResponse({'success': False, 'error': 'Datasource not found for this campaign.'})
    
    # Filter datasources that have an enrichment schema
    datasources = DataSource.objects.filter(schema__enrichment_schema__isnull=False)
    if request.method == 'POST':
        campaign.name = request.POST.get('name')
        campaign.description = request.POST.get('description')
        campaign.save()
        return redirect('campaign_list_create')
    return render(request, 'usex_app/edit_campaign.html', {'campaign': campaign,'projects': projects,'datasources': datasources,'datasource': datasource,'days_list': days_list})

def delete_campaign(request, campaign_id):
    campaign = Campaign.objects.get(id=campaign_id)
    campaign.delete()
    return redirect('campaign_list_create')

def get_profile_templates_for_datasource(request, datasource_id):
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        templates = Templates.objects.filter(datasource=datasource)
        filtered_templates = [
            {
                "id": template.id,
                "name": template.name,
                "expression": template.template_expression,
                "display_text": template.display_text,
                "selections": template.selection_schema,  # Assuming selection_schema is stored as JSON
            }
            for template in templates if "$profile" in template.template_expression and ("$feed" not in template.template_expression and "$aggregation" not in template.template_expression)
        ]
        print(filtered_templates)
        return JsonResponse({"success": True, "templates": filtered_templates})
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
def get_event_templates_for_datasource(request, datasource_id):
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        templates = Templates.objects.filter(datasource=datasource)
        filtered_templates = [
            {
                "id": template.id,
                "name": template.name,
                "expression": template.template_expression,
                "display_text": template.display_text,
                "selections": template.selection_schema,  # Assuming selection_schema is stored as JSON
            }
            for template in templates if ("$feed" in template.template_expression) or ("$aggregation" in template.template_expression)
        ]
        return JsonResponse({"success": True, "templates": filtered_templates})
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
def get_operators_and_enums_by_templateID(request, template_id):
    try:
        # Fetch operators and enums related to the datasource
        template = Templates.objects.get(id=template_id)
        selections = template.selection_schema or []
        selection_dict={}
        for selection in selections:
            unique_id= selection.get('unique_id')
            type= selection.get('type')
            value=selection.get('value').split('.')[-1]
            if type == 'operator':
                operator= Operators.objects.filter(operator_set_id=value)
                options= operator[0].options if operator else []
                selection_dict[unique_id] = {
                    'type': type,
                    'value': value,
                    'options': options
                }
            if type == 'enum':
                enum = Enums.objects.filter(enum_set_id=value)
                options = enum[0].options if enum else []
                selection_dict[unique_id] = {
                    'type': type,
                    'value': value,
                    'options': options
                }
            if type == 'input':
                selection_dict[unique_id] = {
                    'type': type,
                    'value': value,
                    'options': []
                }


        
        return JsonResponse({
            "success": True,
            "operators_and_enums": selection_dict,
            "template_name": template.name,
            "template_expression": template.template_expression,
            "display_text": template.display_text,
            "datasource_id": template.datasource.id,
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})

def create_datasink(request):
    if request.method == "POST":
        datasink_type= request.POST.get('datasink_type')
        form = DataSinkForm(request.POST,datasink_type=datasink_type)

        print('request.POST:', request.POST)
        if form.is_valid():
            connection_params = {}
            print('form.cleaned_data:', form.cleaned_data)
            for key, value in form.cleaned_data.items():
                if key.startswith('connection_params_'):
                    param_name = key.replace('connection_params_', '')
                    connection_params[param_name] = value
            datasink = form.save(commit=False)
            datasink.connection_params = connection_params
            # Check the connection
            try:
                connection = connect_to_data_sink(datasink)
                # If the connection is successful, save the data source
                datasink.save()
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({'success': True, "datasink_id": datasink.id})
                return redirect('datasource_list_create')
            except Exception as e:
                # If the connection fails, add an error to the form
                traceback.print_exc()
                form.add_error(None, f"Connection failed: {str(e)}")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            errors = [str(error) for error in form.non_field_errors()]
            for field, field_errors in form.errors.items():
                errors.extend([f"{field}: {error}" for error in field_errors])
            return JsonResponse({'success': False, 'errors': errors})


        else:
            return JsonResponse({"success": False, "errors": form.errors})
    return JsonResponse({"success": False, "error": "Invalid request method."})
def edit_datasink(request):
    if request.method == 'POST':
        datasink_id = request.POST.get('datasink_id')
        datasink = DataSink.objects.get(pk=datasink_id)
        datasink_type = datasink.datasink_type
        datasink_form = DataSinkForm(request.POST, instance=datasink, datasink_type=datasink_type)
        if datasink_form.is_valid():
            connection_params = {}
            for key, value in datasink_form.cleaned_data.items():
                if key.startswith('connection_params_'):
                    param_name = key.replace('connection_params_', '')
                    connection_params[param_name] = value
            datasink_form.instance.connection_params = connection_params
            try:
                # Check the connection
                connect_to_data_sink(datasink_form.instance)
                datasink_form.save()
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({'success': True})
                return redirect('datasource_list_create')
            except Exception as e:
                traceback.print_exc()
                datasink_form.add_error(None, f"Connection failed: {str(e)}")
        else:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                errors = [str(error) for error in datasink_form.non_field_errors()]
                for field, field_errors in datasink_form.errors.items():
                    errors.extend([f"{field}: {error}" for error in field_errors])
                return JsonResponse({'success': False, 'errors': errors})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def delete_datasink(request):
    if request.method == 'POST':
        datasink_id = request.POST.get('datasink_id')
        try:
            datasink = DataSink.objects.get(pk=datasink_id)
            datasink.delete()
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': True})
            return redirect('datasource_list_create')
        except DataSink.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'DataSink not found.'})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_schema_fields_for_action(request, datasource_id):
    """
    API to fetch schema fields for a given datasource and its related datastores.
    """
    try:
        datasource = DataSource.objects.get(id=datasource_id)

        # Fetch schema fields for datasource
        datasource_fields = list(datasource.schema.enrichment_schema.keys()) if datasource.schema else []

        # Fetch schema fields for all related datastores
        datastore_schemas = {}
        relationships = Relationship.objects.filter(datasource=datasource)
        for relationship in relationships:
            datastore = relationship.datastore
            datastore_schemas[datastore.internal_name] = list(datastore.schema.keys()) if datastore.schema else []
        
        print('datastore_schemas:', datastore_schemas)
        print('datasource_fields:', datasource_fields)
        return JsonResponse({
            "success": True,
            "datasource_fields": datasource_fields,
            "datastore_schemas": datastore_schemas,
        })
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})
    """
    API to fetch schema fields for a given datasource and its related datastores.
    """
    try:
        datasource = DataSource.objects.get(id=datasource_id)

        # Fetch schema fields for datasource
        datasource_fields = datasource.schema.enrichment_schema.keys() if datasource.schema else []

        # Fetch schema fields for all related datastores
        datastore_schemas = {}
        relatioships = Relationship.objects.filter(datasource=datasource)
        for relationship in relatioships:
            datastore=relationship.datastore
            datastore_schemas[datastore.internal_name] = datastore.schema.keys() if datastore.schema else []
        print('datastore_schemas:', datastore_schemas)
        print('datasource_fields:', datasource_fields)
        return JsonResponse({
            "success": True,
            "datasource_fields": datasource_fields,
            "datastore_schemas": datastore_schemas,
        })
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})
def aggregation_view(request, datasource_id):
    """
    View to display the Aggregations page for a specific datasource.
    """
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        

        context = {
            'datasource': datasource,
            
        }
        return render(request, 'usex_app/aggregations.html', context)
    except DataSource.DoesNotExist:
        return render(request, 'usex_app/error.html', {'error': 'Datasource not found.'})
def update_aggregation_schema(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')
            group_by_fields = data.get('group_by_fields')
            formula = data.get('formula')
            result = data.get('result')
            datatype = data.get('datatype')
            retention_window=data.get('retention_window', 1)
            aggregate_type=data.get('aggregation_type')
            tuple_filter=data.get('tuple_filter')

            # Update the parsing_schema
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema:
                # Update or add the field in parsing_schema
                aggregation_schema = schema.aggregation_schema or {}
                aggregation_schema[field_name] = {
                    'aggregate_type': aggregate_type,
                    'group_by_fields': group_by_fields,
                    'retention_window': retention_window,  # Optional field
                    'formula': formula,
                    'result': result,
                    'datatype': datatype,
                    'tuple_filter': tuple_filter
                }
                schema.aggregation_schema = aggregation_schema
                schema.save()
                return JsonResponse({'success': True, 'message': 'Pre-enrichment schema updated successfully.'})
            else:
                print(f"No schema found for DataSource ID: {datasource_id}")
                return JsonResponse({'success': False, 'error': 'Failed to update pre-enrichment schema.'})
            
                
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def delete_aggregation_field(request, datasource_id):
    if request.method == 'POST':
        try:
            # Parse the request payload
            data = json.loads(request.body)
            field_name = data.get('field_name')

            # Fetch the DataSourceSchema associated with the DataSource
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema and field_name in schema.aggregation_schema:
                # Remove the field from enrichment_schema
                del schema.aggregation_schema[field_name]

                
                # Save the updated schema
                schema.save()

                return JsonResponse({'success': True, 'message': f'Field "{field_name}" deleted successfully.'})
            else:
                return JsonResponse({'success': False, 'error': f'Field "{field_name}" not found in enrichment_schema.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def fetch_aggregation_schema(request, datasource_id):
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        aggregation_schema = datasource.schema.aggregation_schema or {}
        return JsonResponse({"success": True, "aggregation_schema": aggregation_schema})
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
    
def get_related_datastores(request):
    """
    API to fetch related datastores based on the selected datasource, including links.
    """
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        
        # Fetch datastores related via Relationships
        relationships = Relationship.objects.filter(datasource=datasource).select_related('datastore')
        related_datastores=[
            relationship.datastore
            for relationship in relationships
        ]
        
        
        # Fetch datastores related via Links
        
        linked_datastores = [
            {
                'key': link.target_datastore.key,
                'name': link.target_datastore.name
            }
            for link in Links.objects.filter(source_datastore__in=related_datastores)
            
        ]
        main_datastores = [
            {
                'key': datastore.key,
                'name': datastore.name
            }
            for datastore in related_datastores
        ]
        # Combine both lists and remove duplicates
        all_related_datastores = {datastore['key']: datastore for datastore in main_datastores + linked_datastores}.values()

        return JsonResponse({'success': True, 'datastores': list(all_related_datastores)})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Datasource not found.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
def delete_link(request):
    """
    API to delete a link by its ID.
    """
    link_id = request.GET.get('link_id')
    print('link_id:', link_id)
    try:
        link = Links.objects.get(id=link_id)
        link.delete()
        return JsonResponse({'success': True})
    except Links.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Link not found.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
def create_project(request):
    """
    API to create a new project.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)  # Parse the JSON payload
            name = data.get('name')
            description = data.get('description', '')

            # Validate the input
            if not name:
                return JsonResponse({'success': False, 'error': 'Project name is required.'})

            # Create the project
            project = CampaignProject.objects.create(name=name, description=description)

            return JsonResponse({'success': True, 'project_id': project.id, 'project_name': project.name})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def save_campaign(request):
    """
    API to save a campaign in the draft stage.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)  # Parse the JSON payload
            campaign_name = data.get('campaignName')
            datasource_id = data.get('dataSource')
            datastore_key = data.get('datastore')
            project_id = data.get('project')

            # Validate the input
            if not campaign_name or not datasource_id or not datastore_key:
                return JsonResponse({'success': False, 'error': 'Campaign Name, DataSource, and Datastore are required.'})

            # Fetch related objects
            datasource = DataSource.objects.get(id=datasource_id)
            datastore = DataStore.objects.get(key=datastore_key)
            project = CampaignProject.objects.get(id=project_id) if project_id else None

            # Create the campaign
            campaign = Campaign.objects.create(
                name=campaign_name,
                description=f"Campaign for {datasource.name}",
                DataSource=datasource,
                datastore=datastore,
                project=project,  # Can be None
                status='draft',
                version='1.0'
            )

            return JsonResponse({'success': True, 'campaign_id': campaign.id})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Datasource not found.'})
        except DataStore.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Datastore not found.'})
        except CampaignProject.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Project not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
    """
    API to save a campaign in the draft stage.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)  # Parse the JSON payload
            campaign_name = data.get('campaignName')
            datasource_id = data.get('dataSource')
            datastore_key = data.get('datastore')
            project_id = data.get('project')

            # Validate the input
            if not campaign_name or not datasource_id or not datastore_key or not project_id:
                return JsonResponse({'success': False, 'error': 'All fields are required.'})

            # Fetch related objects
            datasource = DataSource.objects.get(id=datasource_id)
            datastore = DataStore.objects.get(key=datastore_key)
            project = CampaignProject.objects.get(id=project_id)

            # Create the campaign
            campaign = Campaign.objects.create(
                name=campaign_name,
                description=f"Campaign for {datasource.name}",
                DataSource=datasource,
                datastore=datastore,
                project=project,
                status='draft',
                version='1.0'
            )

            return JsonResponse({'success': True, 'campaign_id': campaign.id})
        except DataSource.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Datasource not found.'})
        except DataStore.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Datastore not found.'})
        except CampaignProject.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Project not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_related_datastores_fields(request):
    """
    API to fetch related datastores based on the selected datasource, including links.
    """
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        
        # Fetch datastores related via Relationships
        relationships = Relationship.objects.filter(datasource=datasource).select_related('datastore')
        related_datastores=[
            relationship.datastore
            for relationship in relationships
        ]
        
        
        # Fetch datastores related via Links
        
        
        
        all_related_datastores=related_datastores+[link.target_datastore for link in Links.objects.filter(source_datastore__in=related_datastores)]
        
        # Combine both lists and remove duplicates
        all_related_datastores_fields =[]
        for datastore in all_related_datastores:
            if datastore.schema:
                schema_fields = list(datastore.schema.keys())
                all_related_datastores_fields.append({
                    'key': datastore.key,
                    'name': datastore.name,
                    'internal_name': datastore.internal_name,
                    'schema': datastore.schema
                })
        
        return JsonResponse({'success': True, 'datastores': list(all_related_datastores_fields)})
    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Datasource not found.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
def get_datasource_fields(request):
    """
    API to fetch fields from the datasource enriched schema and aggregation schema.
    """
    datasource_id = request.GET.get('datasource_id')
    try:
        datasource = DataSource.objects.get(id=datasource_id)
        enrichment_schema = datasource.schema.enrichment_schema
        aggregation_schema = datasource.schema.aggregation_schema

        return JsonResponse({
            "success": True,
            "enriched_schema": enrichment_schema,
            "aggregation_schema": aggregation_schema,
        })
    except DataSource.DoesNotExist:
        return JsonResponse({"success": False, "error": "Datasource not found."})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
def create_expression(profile_filter):
    """
    Create a formula expression from the profile filter JSON.

    Args:
        profile_filter (list): A list of filter blocks containing raw filters and template filters.

    Returns:
        str: The generated formula expression.
    """
    def process_block(block):
        """
        Recursively process a block to generate its formula expression.

        Args:
            block (dict): A block containing filters and nested blocks.

        Returns:
            str: The formula expression for the block.
        """
        block_type = block.get("type")  # AND/OR/Template
        filters = block.get("filters", [])

        expressions = []
        # print(f"block: {block}, block_type: {block_type}, filters: {filters}")
        
        for filter_item in filters:

            if filter_item.get("type") == "template":
                # Process template-based filter
                template_id = filter_item.get("templateId")
                selections = filter_item.get("selections", {})
                template_expression = filter_item.get("template_expression", "")

                # Replace placeholders in the template expression with selection values
                for key, value in selections.items():
                    template_expression = template_expression.replace(f"{key}", str(value))

                expressions.append(f"({template_expression})")
            elif filter_item.get("type") == "raw":
                # Process raw filter
                field = filter_item.get("field")
                operator = filter_item.get("operator")
                
                value = filter_item.get("value")
            

                if field and operator and value:
                    expressions.append(f"({field} {operator} {value})")
            elif filter_item.get("type") in ("OR","AND"):
                expressions.append(process_block(filter_item))
            


        # Encapsulate expressions with AND/OR
        if block_type == "AND":
            return f"({' AND '.join(expressions)})"
        elif block_type == "OR":
            return f"({' OR '.join(expressions)})"
        else:
            return " ".join(expressions)

    # Process the top-level blocks
    formula_expressions = [process_block(block) for block in profile_filter]
    print('formula_expressions:', formula_expressions)
    # Combine all top-level expressions
    return " AND ".join(formula_expressions)
def save_profile_filter(request):
    """
    API to save the profile filter as profile_template_expression.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            profile_filter = data.get('profile_filter')
            print(profile_filter)
            if not profile_filter:
                return JsonResponse({'success': False, 'error': 'Profile template expression is required.'})

            # Validate the profile_filter structure
            if not isinstance(profile_filter, list):
                return JsonResponse({'success': False, 'error': 'Invalid profile filter format. Expected a list of blocks.'})

            # Ensure each block has the required structure
            for block in profile_filter:
                if not isinstance(block, dict) or 'type' not in block or 'filters' not in block:
                    return JsonResponse({'success': False, 'error': 'Invalid block structure in profile filter.'})

            # Ensure each filter has the required structure
            for block in profile_filter:
                for filter_item in block.get('filters', []):
                    if not isinstance(filter_item, dict) or 'type' not in filter_item:
                        return JsonResponse({'success': False, 'error': 'Invalid filter structure in profile filter.'})

            # Save the profile_template_expression to the campaign
            campaign_id = data.get('campaign_id')  # Pass campaign_id as a query parameter
            campaign = Campaign.objects.get(id=campaign_id)
            print('campaign:', campaign)
            print('profile_filter:', profile_filter)
            # Save the profile filter as a JSON string
            campaign.profile_filter = profile_filter
            campaign.profile_template_expression = create_expression(profile_filter)  # Save the filter as a JSON string
            print('campaign.profile_template_expression:', campaign.profile_template_expression)
            campaign.save()

            return JsonResponse({'success': True})
        except Campaign.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Campaign not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def save_trigger_event_filter(request):
    """
    API to save the event trigger filter as event_template_expression.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            event_filter = data.get('event_filter')
            print(event_filter)
            if not event_filter:
                return JsonResponse({'success': False, 'error': 'Event filter is required.'})

            # Validate the event_filter structure
            if not isinstance(event_filter, list):
                return JsonResponse({'success': False, 'error': 'Invalid event filter format. Expected a list of blocks.'})

            # Ensure each block has the required structure
            for block in event_filter:
                if not isinstance(block, dict) or 'type' not in block or 'filters' not in block:
                    return JsonResponse({'success': False, 'error': 'Invalid block structure in event filter.'})

            # Ensure each filter has the required structure
            for block in event_filter:
                for filter_item in block.get('filters', []):
                    if not isinstance(filter_item, dict) or 'type' not in filter_item:
                        return JsonResponse({'success': False, 'error': 'Invalid filter structure in event filter.'})

            # Save the event_template_expression to the campaign
            campaign_id = data.get('campaign_id')  # Pass campaign_id as a query parameter
            campaign = Campaign.objects.get(id=campaign_id)
            campaign.event_filter = event_filter
            campaign.event_template_expression = create_expression(event_filter)  # Generate the expression
            print('campaign:', campaign)
            print('event_filter:', event_filter)
            print('campaign.event_template_expression:', campaign.event_template_expression)
            campaign.save()

            return JsonResponse({'success': True})
        except Campaign.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Campaign not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})

def save_trigger_actions(request):
    """
    API to save trigger actions and associate them with the Campaign model.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            trigger_actions = data.get('trigger_actions')
            campaign_id = data.get('campaign_id')

            print('trigger_actions:', trigger_actions)

            if not trigger_actions or not campaign_id:
                return JsonResponse({'success': False, 'error': 'Campaign ID and Trigger actions are required.'})

            # Fetch the campaign
            campaign = Campaign.objects.get(id=campaign_id)

            # Validate and process each trigger action
            for action_data in trigger_actions:
                event_identifier = action_data.get('eventIdentifier')
                endpoint_ids = action_data.get('endpoint', [])
                profile_attributes = action_data.get('profileAttributes', {})
                feed_attributes = action_data.get('realtimeAttributes', {})

                if not event_identifier or not endpoint_ids:
                    return JsonResponse({'success': False, 'error': 'Invalid trigger action structure. Event Identifier and Endpoint are required.'})

                # Create or update the Action
                action, created = Action.objects.get_or_create(
                    identifier=event_identifier,
                    defaults={
                        'name': action_data.get('name', event_identifier),
                        'profile_attributes': profile_attributes,
                        'feed_attributes': feed_attributes,
                    }
                )

                if not created:
                    # Update existing action attributes
                    action.profile_attributes = profile_attributes
                    action.feed_attributes = feed_attributes
                    action.save()

                # Associate DataSink objects with the Action
                datasinks = DataSink.objects.filter(id__in=endpoint_ids)
                action.endpoint.set(datasinks)

                # Add the Action to the Campaign
                campaign.actions.add(action)

            campaign.save()

            return JsonResponse({'success': True, 'message': 'Trigger actions saved successfully.'})
        except Campaign.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Campaign not found.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_datasinks(request):
    """
    API to fetch DataSink options for the endpoint dropdown.
    """
    try:
        datasinks = DataSink.objects.all()
        datasink_data = [
            {"id": datasink.id, "name": datasink.name, "type": datasink.datasink_type}
            for datasink in datasinks
        ]
        return JsonResponse({"success": True, "datasinks": datasink_data})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
def save_contact_policy(request):
    """
    API to save the contact policy for a campaign.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            campaign_id = data.get('campaign_id')
            contact_policy = data.get('contact_policy')

            if not campaign_id or not contact_policy:
                return JsonResponse({'success': False, 'error': 'Campaign ID and Contact Policy are required.'})

            # Fetch the campaign
            campaign = get_object_or_404(Campaign, id=campaign_id)

            # Save the contact policy
            campaign.contact_policy = contact_policy
            campaign.save()

            return JsonResponse({'success': True, 'message': 'Contact policy saved successfully.'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_campaign_details(request, campaign_id):
    """
    API to fetch campaign details for editing.
    """
    try:
        campaign = Campaign.objects.get(id=campaign_id)
        campaign_data = {
            "profile_filter": campaign.profile_filter,
            "event_filter": campaign.event_filter,
            "trigger_actions": [
                {
                    "id": action.id,
                    "name": action.name,
                    "identifier": action.identifier,
                    "profile_attributes": action.profile_attributes,
                    "feed_attributes": action.feed_attributes,
                    "endpoint": [
                        {"id": datasink.id, "name": datasink.name}
                        for datasink in action.endpoint.all()
                    ],
                }
                for action in campaign.actions.all()
            ],
            "contact_policy": campaign.contact_policy,
        }
        return JsonResponse({"success": True, "campaign": campaign_data})
    except Campaign.DoesNotExist:
        return JsonResponse({"success": False, "error": "Campaign not found."})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})
def review_campaign(request, campaign_id):
    campaign = get_object_or_404(Campaign, id=campaign_id)
    campaign_data = {
            "profile_filter": campaign.profile_filter,
            "event_filter": campaign.event_filter,
            "trigger_actions": [
                {
                    "id": action.id,
                    "name": action.name,
                    "identifier": action.identifier,
                    "profile_attributes": action.profile_attributes,
                    "feed_attributes": action.feed_attributes,
                    "endpoint": [
                        {"id": datasink.id, "name": datasink.name}
                        for datasink in action.endpoint.all()
                    ],
                }
                for action in campaign.actions.all()
            ],
            "contact_policy": campaign.contact_policy,
        }
    return render(request, 'usex_app/review_campaign.html', {'campaign': campaign_data})

def approve_campaign(request, campaign_id):
    if request.method == 'POST':
        campaign = get_object_or_404(Campaign, id=campaign_id)
        campaign.status = 'approved'
        campaign.save()
        return JsonResponse({'success': True})
    return JsonResponse({'success': False, 'error': 'Invalid request method'})

def reject_campaign(request, campaign_id):
    if request.method == 'POST':
        campaign = get_object_or_404(Campaign, id=campaign_id)
        reason = request.POST.get('reason', '')
        campaign.status = 'rejected'
        campaign.rejection_reason = reason
        campaign.save()
        return JsonResponse({'success': True})
    return JsonResponse({'success': False, 'error': 'Invalid request method'})


def system_health(request):
    """
    View to display the health status of datastore Redis, aggregate Redis, and Kafka with lag, last offset, and latest offset.
    """
    health_status = {
        "datastore_redis": {},
        "aggregate_redis": {},
        "kafka": {},
    }

    # Check Datastore Redis Health
    try:
        datastore_redis = redis.StrictRedis(
            host=settings.DATASTORE_REDIS_HOST,
            port=settings.DATASTORE_REDIS_PORT,
            db=settings.DATASTORE_REDIS_DB,
            password=settings.DATASTORE_REDIS_PASSWORD or None,
        )
        datastore_redis.ping()
        health_status["datastore_redis"] = {"status": "healthy"}
    except Exception as e:
        health_status["datastore_redis"] = {"status": "unhealthy", "error": str(e)}

    # Check Aggregate Redis Health
    try:
        aggregate_redis = redis.StrictRedis(
            host=settings.AGGREGATE_REDIS_HOST,
            port=settings.AGGREGATE_REDIS_PORT,
            db=settings.AGGREGATE_REDIS_DB,
            password=settings.AGGREGATE_REDIS_PASSWORD or None,
        )
        aggregate_redis.ping()
        health_status["aggregate_redis"] = {"status": "healthy"}
    except Exception as e:
        health_status["aggregate_redis"] = {"status": "unhealthy", "error": str(e)}

    # Check Kafka Health
    try:
        kafka_admin = AdminClient({"bootstrap.servers": settings.KAFKA_BROKER_URL})
        kafka_consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BROKER_URL,
            'group.id': 'health-check-group',
            'enable.auto.commit': False
        })

        cluster_metadata = kafka_admin.list_topics(timeout=10)
        topics = cluster_metadata.topics
        kafka_details = {}

        for topic_name, topic_metadata in topics.items():
            partitions = {}
            if topic_name.startswith("_"):  # Skip internal topics
                continue
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                # Create a TopicPartition object
                topic_partition = TopicPartition(topic_name, partition_id)

                # Get watermark offsets
                low, high = kafka_consumer.get_watermark_offsets(topic_partition, timeout=5.0)

                # Calculate lag
                lag = high - low

                partitions[partition_id] = {
                    "lag": lag,
                    "last_offset": low,
                    "latest_offset": high,
                }

            kafka_details[topic_name] = partitions

        health_status["kafka"] = {
            "status": "healthy",
            "topics": kafka_details,
        }
    except KafkaException as e:
        health_status["kafka"] = {"status": "unhealthy", "error": str(e)}
    except Exception as e:
        health_status["kafka"] = {"status": "unhealthy", "error": traceback.format_exc()}

    return render(request, "usex_app/system_health.html", {"health_status": health_status})
def performance_tuning(request):
    for ds in DataSource.objects.all():
        enricher_params = ds.enricher_params or {}
        computed_time=enricher_params.get('computed_time',None)
        if computed_time:
            if (datetime.now()-datetime.strptime(computed_time,"%Y-%m-%d %H:%M:%S")).total_seconds() > 3600*48:  # If computed time is more than 48 hours ago
                enricher_params=generate_enricher_params(ds)
                ds.enricher_params = enricher_params
                ds.save()
        else:
            enricher_params=generate_enricher_params(ds)
            ds.enricher_params = enricher_params
            ds.save()
    datasources = DataSource.objects.all().values(
        'id', 'name', 'enricher_params'
    )
    print(datasources)
    for ds in datasources:
        # Extract enricher parameters
        enricher_params = ds.get('enricher_params', {})
        
        ds['min_pods'] = enricher_params.get('min_pods', 1)
        ds['max_pods'] = enricher_params.get('max_pods', 2)
        ds['cpu_per_pod'] = enricher_params.get('cpu_per_pod', 0.2)
        ds['memory_per_pod'] = enricher_params.get('memory_per_pod', 256)
        ds['enricher_params'] = {k:v for k,v in enricher_params.items() if k not in ['min_pods', 'max_pods', 'cpu_per_pod', 'memory_per_pod','computed_time'] }
    # Calculate totals and averages
    total_min_pods = sum(ds['min_pods'] for ds in datasources)
    total_max_pods = sum(ds['max_pods'] for ds in datasources)
    avg_cpu_per_pod = round(sum(ds['cpu_per_pod'] for ds in datasources) / len(datasources), 1)
    avg_memory_per_pod = round(sum(ds['memory_per_pod'] for ds in datasources) / len(datasources), 1)

    # Calculate recommended pods
    recommended_pods = 0
    for ds in datasources:
        if 'messages_per_second' in ds['enricher_params']:
            # Kafka: Calculate based on messages per second and message size
            recommended_pods += (ds['enricher_params']['messages_per_second'] * ds['enricher_params']['avg_message_size_bytes']) // (1024 * 1024 * 10)  # Assuming 10 MB per pod
        elif 'row_count' in ds['enricher_params']:
            # Database: Calculate based on row count and row size
            recommended_pods += (ds['enricher_params']['row_count'] * ds['enricher_params']['avg_row_size_kb']) // (1024 * 10)  # Assuming 10 MB per pod
        elif 'total_records' in ds['enricher_params']:
            # CSV: Calculate based on total records and size
            recommended_pods += ds['enricher_params']['total_size_kb'] // (1024 * 10)  # Assuming 10 MB per pod

    recommended_pods = max(1, int(recommended_pods))  # Ensure at least 1 pod is recommended

    context = {
        'datasources': datasources,
        'total_min_pods': total_min_pods,
        'total_max_pods': total_max_pods,
        'avg_cpu_per_pod': avg_cpu_per_pod,
        'avg_memory_per_pod': avg_memory_per_pod,
        'recommended_pods': recommended_pods,
    }
    return render(request, 'usex_app/performance_tuning.html', context)
import math

def calculate_optimum_pods(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        datasource_id = data.get('id')
        datasource_type = data.get('datasource_type')
        params = {key: float(value) for key, value in data.items() if key not in ['id', 'datasource_type']}

        # Default values
        min_pods = 1
        max_pods = 2
        cpu_per_pod = 0.2
        memory_per_pod = 256

        # Calculation logic based on datasource type
        if datasource_type == 'Kafka':
            num_partitions = params.get('num_partitions', 0)
            messages_per_second = params.get('messages_per_second', 0)
            avg_message_size_bytes = params.get('avg_message_size_bytes', 0)

            if num_partitions and messages_per_second and avg_message_size_bytes:
                total_message_size_per_second = messages_per_second * avg_message_size_bytes
                min_pods = math.ceil(total_message_size_per_second / (1024 * 1024 * 10))  # Assuming 10 MB per pod
                max_pods = min_pods + 2
                cpu_per_pod = 0.5
                memory_per_pod = 512

        elif datasource_type in ['Postgres', 'Mysql']:
            row_count = params.get('row_count', 0)
            avg_row_size_kb = params.get('avg_row_size_kb', 0)

            if row_count and avg_row_size_kb:
                total_data_size_mb = (row_count * avg_row_size_kb) / 1024  # Convert KB to MB
                min_pods = math.ceil(total_data_size_mb / 10)  # Assuming 10 MB per pod
                max_pods = min_pods + 1
                cpu_per_pod = 0.3
                memory_per_pod = 256

        elif datasource_type == 'CSV':
            total_records = params.get('total_records', 0)
            total_size_kb = params.get('total_size_kb', 0)

            if total_records and total_size_kb:
                total_data_size_mb = total_size_kb / 1024  # Convert KB to MB
                min_pods = math.ceil(total_data_size_mb / 10)  # Assuming 10 MB per pod
                max_pods = min_pods + 1
                cpu_per_pod = 0.2
                memory_per_pod = 128

        # Compute totals and averages (example logic)
        total_min_pods = min_pods  # Replace with actual total calculation
        total_max_pods = max_pods  # Replace with actual total calculation
        avg_cpu_per_pod = round(cpu_per_pod, 1)  # Replace with actual average calculation
        avg_memory_per_pod = round(memory_per_pod, 1)  # Replace with actual average calculation

        return JsonResponse({
            'min_pods': min_pods,
            'max_pods': max_pods,
            'cpu_per_pod': cpu_per_pod,
            'memory_per_pod': memory_per_pod,
            'total_min_pods': total_min_pods,
            'total_max_pods': total_max_pods,
            'avg_cpu_per_pod': avg_cpu_per_pod,
            'avg_memory_per_pod': avg_memory_per_pod,
        })

    return JsonResponse({'success': False, 'error': 'Invalid request method.'})
def get_datasource_and_related_details(request, datasource_id):
    """
    API to fetch datasource details along with related and linked datastore details.
    """
    try:
        # Fetch the datasource
        datasource = DataSource.objects.get(id=datasource_id)

        # Fetch related datastores via relationships
        relationships = Relationship.objects.filter(datasource=datasource).select_related('datastore')
        related_datastores = [
            {
                'id': relationship.datastore.id,
                'name': relationship.datastore.name,
                'internal_name': relationship.datastore.internal_name,
                'key': relationship.datastore.key,
                'schema': relationship.datastore.schema or {},
                'datasource_key': relationship.datasource_key,
                'linked_datastores': [
                    {
                        'id': link.target_datastore.id,
                        'name': link.target_datastore.name,
                        'internal_name': link.target_datastore.internal_name,
                        'key': link.target_datastore.key,
                        'schema': link.target_datastore.schema or {},
                        'source_column': link.source_column,
                        'target_column': link.target_column,
                    }
                    for link in Links.objects.filter(source_datastore=relationship.datastore)
                ]
            }
            for relationship in relationships
        ]

        # Fetch linked datastores via links
        

        # Combine related and linked datastores, removing duplicates
        # all_datastores = {ds['id']: ds for ds in related_datastores + linked_datastores}.values()

        # Prepare the response
        response_data = {
            'datasource': {
                'id': datasource.id,
                'name': datasource.name,
                'datasource_type': datasource.datasource_type,
                'internal_name': datasource.internal_name,
                'connection_params': datasource.connection_params,
                'input_schema': datasource.schema.input_schema if datasource.schema else {},
                'parsing_schema': datasource.schema.parsing_schema if datasource.schema else {},
                'enrichment_schema': datasource.schema.enrichment_schema if datasource.schema else {},
                'aggregation_schema': datasource.schema.aggregation_schema if datasource.schema else {},
                'rejection_fields': datasource.schema.rejection_fields if datasource.schema else {},
                'enrichment_rejection_fields': datasource.schema.enrichment_rejection_fields if datasource.schema else {},
                'enricher_params': datasource.enricher_params or {},
                'skip_campaign_processing': datasource.skip_campaign_processing,
                'datastores': list(related_datastores)
            },
            
            

        }

        return JsonResponse({'success': True, 'data': response_data})

    except DataSource.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Datasource not found.'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
