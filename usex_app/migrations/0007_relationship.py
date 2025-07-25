# Generated by Django 4.2.21 on 2025-06-02 09:32

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('usex_app', '0006_datastore'),
    ]

    operations = [
        migrations.CreateModel(
            name='Relationship',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('datasource_key', models.CharField(default='default_key', help_text='Key from the DataSource used to access the DataStore.', max_length=100)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('datasource', models.ForeignKey(help_text='The DataSource associated with this relationship.', on_delete=django.db.models.deletion.CASCADE, related_name='relationships', to='usex_app.datasource')),
                ('datastore', models.ForeignKey(help_text='The DataStore associated with this relationship.', on_delete=django.db.models.deletion.CASCADE, related_name='relationships', to='usex_app.datastore')),
            ],
            options={
                'unique_together': {('datasource', 'datastore')},
            },
        ),
    ]
