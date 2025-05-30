from django.contrib import admin
from .models import DataSource,DataSourceSchema
# Register your models here.
@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    pass
@admin.register(DataSourceSchema)
class DataSourceSchemaAdmin(admin.ModelAdmin):
    pass