import os
from pathlib import Path

# NETWORK & ROUTING
ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', '127.0.0.1').strip(',').split(',')
WSGI_APPLICATION = 'config.wsgi.application'
ROOT_URLCONF = 'config.urls'
INTERNAL_IPS = os.environ.get('INTERNAL_IPS', '127.0.0.1').strip(',').split(',')
CSRF_TRUSTED_ORIGINS = os.environ.get('CSRF_ORIGINS', 'http://127.0.0.1').strip(',').split(',')

# RUNTIME
BASE_DIR = Path(__file__).resolve().parent.parent
DEBUG = os.environ.get('DEBUG', False) == 'True'
SECRET_KEY = os.environ.get('SECRET_KEY')
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATIC_URL = 'static/'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'movies.api.v1.pagination.NumberPaginationNoLinks',
    'PAGE_SIZE': 50
}
APPEND_SLASH = True

# I18N & CUSTOMISATION
LANGUAGE_CODE = 'ru-RU'
LOCALE_PATHS = ['movies/locale']
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True
