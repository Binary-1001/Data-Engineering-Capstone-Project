FROM postgres:13
ENV POSTGRES_PASSWORD=postgres
COPY ./01_init_system.sh /docker-entrypoint-initdb.d/
COPY ./02_init_hvs.sh /docker-entrypoint-initdb.d/
EXPOSE 5432
