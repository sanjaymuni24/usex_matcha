from django.contrib import admin
from .models import DataSource
# Register your models here.
@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    pass
