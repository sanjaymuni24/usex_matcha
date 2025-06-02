from django import forms
from .models import DataSource,DataStore

class DataSourceForm(forms.ModelForm):
    class Meta:
        model = DataSource
        fields = ['name', 'description', 'datasource_type']
    def __init__(self, *args, **kwargs):
    # Extract the datasource_type from kwargs if provided
        print('kwargs:', kwargs)
        print('args:', args)
        datasource_type = kwargs.pop('datasource_type', None)
        super().__init__(*args, **kwargs)

        # Add dynamic fields for connection parameters based on datasource_type
        if datasource_type:
            metadata = DataSource.CONNECTION_PARAMS_METADATA.get(datasource_type, {'mandatory': [], 'optional': []})
            mandatory_fields = metadata['mandatory']
            optional_fields = metadata['optional']

            # Add mandatory fields
            for field in mandatory_fields:
                self.fields[f'connection_params_{field}'] = forms.CharField(
                    required=True,
                    label=field.capitalize(),
                    widget=forms.TextInput(attrs={'class': 'form-control'})
                )

            # Add optional fields
            for field in optional_fields:
                self.fields[f'connection_params_{field}'] = forms.CharField(
                    required=False,
                    label=field.capitalize(),
                    widget=forms.TextInput(attrs={'class': 'form-control'})
                )
class DataStoreForm(forms.ModelForm):
    class Meta:
        model = DataStore
        fields = ['name', 'description', 'key']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter datastore name'}),
            'description': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter datastore description'}),
            'key': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter unique key'}),
        }