from django.shortcuts import render
from .models import DataSource
from .forms import DataSourceForm
import  json
from django.shortcuts import redirect
from django.http import JsonResponse
from .utility.data_source_connection import connect_to_data_source
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
def data_source_update(request, pk):
    """
    Update an existing data source.
    """
    data_source = DataSource.objects.get(pk=pk)
    if request.method == 'POST':
        form = DataSourceForm(request.POST, instance=data_source)
        if form.is_valid():
            form.save()
            return redirect('data_source_list')
    else:
        form = DataSourceForm(instance=data_source)
    return render(request, 'usex_app/data_source_form.html', {'form': form})
def data_source_delete(request, pk):
    """
    Delete an existing data source.
    """
    data_source = DataSource.objects.get(pk=pk)
    if request.method == 'POST':
        data_source.delete()
        return redirect('data_source_list')
    return render(request, 'usex_app/data_source_confirm_delete.html', {'data_source': data_source})
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

            # Save the DataSource instance
            datasource = form.save(commit=False)
            datasource.connection_params = connection_params
            datasource.save()
            return redirect('datasource_list')
    else:
        form = DataSourceForm()

    datasources = DataSource.objects.all()
    connection_params_metadata = json.dumps(DataSource.CONNECTION_PARAMS_METADATA)

    return render(request, 'usex_app/datasource.html', {
        'datasources': datasources,
        'form': form,
        'connection_params_metadata': connection_params_metadata,
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
