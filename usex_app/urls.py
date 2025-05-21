from django.urls import path
from . import views

urlpatterns = [
    path('datasources/', views.datasource_list_create, name='datasource_list_create'),
    path('delete_datasource/<int:datasource_id>/', views.delete_datasource, name='delete_datasource'),
    path('edit_datasource/', views.edit_datasource, name='edit_datasource'),
    path('home/', views.home, name='home'),
    path('enrichments/', views.enrichments, name='enrichments'),
    path('query-data/', views.query_data, name='query_data'),
]