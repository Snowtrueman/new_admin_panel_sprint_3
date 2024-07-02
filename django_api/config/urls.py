from django.contrib import admin
from django.urls import path, include
from django.conf.urls.static import static

from config.components import common

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('movies.api.urls')),
] + static(common.STATIC_URL, document_root=common.STATIC_ROOT)
