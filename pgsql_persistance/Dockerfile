FROM postgres:13
ENV POSTGRES_PASSWORD=postgres
COPY ./init_persistence.sh /docker-entrypoint-initdb.d/
COPY ./migrations.sql /docker-entrypoint-initdb.d/
COPY ./analytics_schema.sql /docker-entrypoint-initdb.d/
COPY ./indexes.sql /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init_persistence.sh
EXPOSE 5432