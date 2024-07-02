FROM python:3.11

RUN groupadd -r web && useradd -r -g web web && mkdir -p /var/www/app/ && chown -R web:web /var/www/app/

WORKDIR /var/www/app/

COPY requirements.txt manage.py uwsgi/uwsgi.ini ./
COPY entrypoint.sh /root
RUN chmod +x /root/entrypoint.sh

RUN apt-get update && apt-get -y dist-upgrade && apt-get install -y ncat
RUN  pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY movies/ movies/

EXPOSE 8000

ENTRYPOINT ["/root/entrypoint.sh"]