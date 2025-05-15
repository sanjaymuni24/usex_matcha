from django.urls import path
from . import views

urlpatterns = [
    path('datasources/', views.datasource_list_create, name='datasource_list'),
]