FROM python:3.11

RUN groupadd -r etl && useradd -r -g etl etl && mkdir -p /var/www/etl/ && chown etl:etl /var/www/etl/
WORKDIR /var/www/etl/

COPY requirements.txt entrypoint.sh ./
RUN chmod +x entrypoint.sh

RUN apt-get update && apt-get -y dist-upgrade && apt-get install -y ncat
RUN  pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY clients/ clients/
COPY common/ common/
COPY pipelines/ pipelines/
COPY __init__.py settings.py start_etl.py elastic_schema.json ./

USER etl
ENTRYPOINT ["/var/www/etl/entrypoint.sh"]
