from django.shortcuts import render
from .models import DataSource,DataSourceSchema
from .forms import DataSourceForm
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
    return redirect('datasource_list')

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
        datasource_type = request.POST.get('datasource_type')
        form = DataSourceForm(request.POST, datasource_type=datasource_type)
        if form.is_valid():
            # Extract connection_params from the dynamic fields
            connection_params = {}
            for key, value in form.cleaned_data.items():
                if key.startswith('connection_params_'):
                    param_name = key.replace('connection_params_', '')
                    connection_params[param_name] = value

            # Create a temporary DataSource instance to validate the connection
            datasource = form.save(commit=False)
            datasource.connection_params = connection_params

            # Check the connection
            try:
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
        form = DataSourceForm()

    datasources = DataSource.objects.all()
    connection_params_metadata = json.dumps(DataSource.CONNECTION_PARAMS_METADATA)
    connection_params_values = {ds.id: ds.connection_params for ds in datasources}

    return render(request, 'usex_app/datasource.html', {
        'datasources': datasources,
        'form': form,
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
    return render(request, 'usex_app/enrichments.html')
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
    return render(request, 'usex_app/enrichments.html', {'datasource': datasource})
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
                    pre_enrichment_schema={}
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
