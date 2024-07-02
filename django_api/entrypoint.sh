#!/bin/sh

# Роман, приветствую! Виделись в прошлом году на async python :)
# Вообще, план был такой, что API никогда не поднимется (healthcheck), пока успешно не отработает парсер,
# а парсер как раз, в свою очередь, дождется пока postgres не опубликует порт. Т.е. к моменту запуска API будет готова
# и база и данные в ней.
# Но ок, для надёжности, добавлю и сюда :)

echo "Waiting for postgres..."
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

python manage.py collectstatic --no-input

chown web:web /var/www/app/static/

su web

python manage.py migrate --fake movies
python manage.py migrate

python manage.py createsuperuser --noinput --username admin --email admin@admin.com

uwsgi --strict --ini uwsgi.ini

exec "$@"