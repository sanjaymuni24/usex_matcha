from django.urls import path
from . import views

urlpatterns = [
    path('datasources/', views.datasource_list_create, name='datasource_list_create'),
    path('delete_datasource/<int:datasource_id>/', views.delete_datasource, name='delete_datasource'),
    path('edit_datasource/', views.edit_datasource, name='edit_datasource'),
    path('home/', views.home, name='home'),
    path('enrichments/', views.enrichments, name='enrichments'),
    path('query-data/', views.query_data, name='query_data'),
    
    path('enrichment/<int:datasource_id>/', views.enrichment_view, name='enrichment'),
    path('fetch-query-dataset/<int:datasource_id>/', views.fetch_query_dataset, name='fetch_query_dataset'),


    path('api/formula_interpreter/', views.formula_interpreter_api, name='formula_interpreter_api'),
    path('api/update_schema/<int:datasource_id>/', views.update_schema, name='update_schema'),
    path('api/get_operations', views.get_operations, name='get_operations'),
]