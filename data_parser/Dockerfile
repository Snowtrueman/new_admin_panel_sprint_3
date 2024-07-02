FROM python:3.11

RUN groupadd -r web && useradd -r -g web web && mkdir -p /var/www/parser/ && chown web:web /var/www/parser/
WORKDIR /var/www/parser/

COPY requirements.txt entrypoint.sh ./
RUN chmod +x entrypoint.sh

RUN apt-get update && apt-get -y dist-upgrade && apt-get install -y ncat
RUN  pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY parser/ parser/

USER web
ENTRYPOINT ["/var/www/parser/entrypoint.sh"]
