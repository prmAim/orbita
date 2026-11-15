FROM postgres:17.11-alpine@sha256:e5d2edd28b1f8d40a0a6c0c9cdc545b19b56a0d5d88f94bc11a97d970d4700d0
COPY deploy/postgres-init.sql /docker-entrypoint-initdb.d/010-orbita-role.sql
COPY deploy/postgres-check-passwords.sh /docker-entrypoint-initdb.d/000-orbita-passwords.sh
RUN sed -i 's/\r$//' /docker-entrypoint-initdb.d/000-orbita-passwords.sh && chmod 755 /docker-entrypoint-initdb.d/000-orbita-passwords.sh
