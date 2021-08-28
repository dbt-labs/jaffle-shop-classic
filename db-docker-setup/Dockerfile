FROM postgres:12.8-alpine3.14

COPY ./setup-db.sh /docker-entrypoint-initdb.d/setup-db.sh

RUN chmod 0666 /docker-entrypoint-initdb.d/setup-db.sh