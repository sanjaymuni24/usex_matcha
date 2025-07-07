from django.shortcuts import render
from .models import DataSource,DataSourceSchema,DataStore,Relationship,Enums,Templates,Operators,Links,Campaign
from .forms import DataSourceForm,DataStoreForm
import  json
from django.shortcuts import redirect,get_object_or_404
from django.http import JsonResponse
from .utility.data_source_connection import connect_to_data_source,query_dataset
from .utility.operators import ColumnOperatorsWrapper, FormulaInterpreter

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

    # Fetch all datasources and datastores
    datasources = DataSource.objects.all()
    datastores = DataStore.objects.all()

    # Prepare metadata for connection parameters
    connection_params_metadata = json.dumps(DataSource.CONNECTION_PARAMS_METADATA)
    connection_params_values = {ds.id: ds.connection_params for ds in datasources}

    return render(request, 'usex_app/datasource.html', {
        'datasources': datasources,
        'datastores': datastores,
        'datasource_form': datasource_form,
        'datastore_form': datastore_form,
        'connection_params_metadata': connection_params_metadata,
        'connection_params_values': json.dumps(connection_params_values),
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

            # Update the parsing_schema
            datasource = DataSource.objects.get(id=datasource_id)
            schema = datasource.schema

            if schema:
                # Update or add the field in parsing_schema
                enrichment_schema = schema.enrichment_schema or {}
                enrichment_schema[field_name] = {
                    'formula': formula,
                    'result': result,
                    'datatype': datatype
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
    return render(request, 'usex_app/campaigns.html', {'campaigns': campaigns})
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
    return render(request, 'usex_app/create_campaign.html', {'datasources': datasources,'days_list': days_list})
def edit_campaign(request, campaign_id):
    campaign = Campaign.objects.get(id=campaign_id)
    if request.method == 'POST':
        campaign.name = request.POST.get('name')
        campaign.description = request.POST.get('description')
        campaign.save()
        return redirect('campaign_list_create')
    return render(request, 'usex_app/edit_campaign.html', {'campaign': campaign})

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
            for template in templates if "$profile" in template.template_expression and "$feed" not in template.template_expression
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
            for template in templates if "$feed" in template.template_expression 
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
            "operators_and_enums": selection_dict
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})