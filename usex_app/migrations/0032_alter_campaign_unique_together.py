# Generated by Django 4.2.21 on 2025-07-10 15:29

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('usex_app', '0031_campaign_project'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='campaign',
            unique_together={('name', 'DataSource', 'project')},
        ),
    ]
