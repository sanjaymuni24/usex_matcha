# Generated by Django 4.2.21 on 2025-07-03 05:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('usex_app', '0021_alter_enums_datatype'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='operators',
            name='category',
        ),
        migrations.RemoveField(
            model_name='operators',
            name='description',
        ),
        migrations.RemoveField(
            model_name='operators',
            name='supported_datatypes_left',
        ),
        migrations.RemoveField(
            model_name='operators',
            name='supported_datatypes_right',
        ),
        migrations.AddField(
            model_name='operators',
            name='operator_set_id',
            field=models.CharField(blank=True, default=None, help_text='Unique ID for the enum set.', max_length=100, null=True, unique=True),
        ),
        migrations.AlterField(
            model_name='operators',
            name='options',
            field=models.JSONField(default=dict, help_text='Options for the enum set as key-value pairs.'),
        ),
    ]
