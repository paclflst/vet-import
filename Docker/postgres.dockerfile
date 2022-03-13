FROM postgres
ENV POSTGRES_HOST_AUTH_METHOD trust
ENV POSTGRES_DB vet_import
COPY psql_dump.sql /docker-entrypoint-initdb.d/
EXPOSE 5432