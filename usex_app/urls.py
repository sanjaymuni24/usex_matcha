from django.urls import path
from . import views

urlpatterns = [
    path('datasources/', views.datasource_list_create, name='datasource_list_create'),
    path('delete_datasource/<int:datasource_id>/', views.delete_datasource, name='delete_datasource'),
    path('edit_datasource/', views.edit_datasource, name='edit_datasource'),
    path('home/', views.home, name='home'),
    path('enrichments/', views.enrichments, name='enrichments'),
    path('query-data/', views.query_data, name='query_data'),
    path('relationships/', views.relationship_view, name='relationships'),    
    path('get-pre-enrichment-schema/', views.get_pre_enrichment_schema, name='get_pre_enrichment_schema'),
    path('get-relationships/', views.get_relationships, name='get_relationships'),
    path('create-relationship/', views.create_relationship, name='create_relationship'),
    path('enrichment/<int:datasource_id>/', views.enrichment_view, name='enrichment'),
    path('fetch-query-dataset/<int:datasource_id>/', views.fetch_query_dataset, name='fetch_query_dataset'),
    path('api/create_datastore/', views.create_datastore, name='create_datastore'),
    path('api/delete_datastore/<int:datastore_id>/', views.delete_datastore, name='delete_datastore'),
    path('api/edit-datastore/', views.edit_datastore, name='edit_datastore'),
    path('api/formula_interpreter/', views.formula_interpreter_api, name='formula_interpreter_api'),
    path('api/update_schema/<int:datasource_id>/', views.update_schema, name='update_schema'),
    path('api/update_pre_enrichment_schema/<int:datasource_id>/', views.update_pre_enrichment_schema, name='update_pre_enrichment_schema'),
    path('api/get_operations', views.get_operations, name='get_operations'),
    path('api/delete_pre_enrichment_field/<int:datasource_id>/', views.delete_pre_enrichment_field, name='delete_pre_enrichment_field'),
    path('get-input-schema/', views.get_input_schema, name='get_input_schema'),
    path('get-rejection-schema/', views.get_rejection_schema, name='get_rejection_schema'),
    path('update-rejection-schema/', views.update_rejection_schema, name='update_rejection_schema'),
    path('delete-rejection-field/', views.delete_rejection_field, name='delete_rejection_field'),
]