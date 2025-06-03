from django.shortcuts import render
from .models import DataSource,DataSourceSchema,DataStore,Relationship
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

def enrichment_view(request, datasource_id):
    datasource = get_object_or_404(DataSource, id=datasource_id)
    return render(request, 'usex_app/parser.html', {'datasource': datasource})
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
def update_pre_enrichment_schema(datasource_id, field_name, formula, result, datatype):
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
def update_pre_enrichment_schema(request, datasource_id):
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
def delete_pre_enrichment_field(request, datasource_id):
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

def relationship_view(request):
    datasources = DataSource.objects.filter(schema__pre_enrichment_schema__isnull=False)
    return render(request, 'usex_app/relationship.html', {'datasources': datasources})

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
def get_relationships(request):
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