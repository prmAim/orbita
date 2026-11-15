FROM node:22.23.5-alpine AS frontend
WORKDIR /work/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM eclipse-temurin:25-jdk-alpine AS build
WORKDIR /work/backend
COPY backend/ ./
COPY --from=frontend /work/backend/src/main/resources/static/ src/main/resources/static/
RUN --mount=type=cache,target=/root/.m2 chmod +x mvnw && ./mvnw -B -DskipTests package

FROM eclipse-temurin:25-jre-alpine AS java
FROM postgres:17.11-alpine@sha256:e5d2edd28b1f8d40a0a6c0c9cdc545b19b56a0d5d88f94bc11a97d970d4700d0 AS runtime
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:$PATH"
WORKDIR /app
COPY --from=build /work/backend/target/orbita-0.0.1-SNAPSHOT.jar /app/orbita.jar
RUN mkdir -p /var/lib/orbita/backups /usb && chown -R 65534:65534 /app /var/lib/orbita /usb
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/orbita.jar"]
