from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    
    path('admin/', admin.site.urls),
    path('', include('usex_app.urls'))
    ]
    # Include other app URLs here
