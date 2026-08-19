# Multi-stage build so the image is fully self-contained: the Spring Boot jar is
# built from source inside the image, so no pre-built target/*.jar is required.
# This works both locally and on Hugging Face Spaces (where target/ is not present
# because it is gitignored).

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Resolve dependencies first for better layer caching.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline || true

COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=build /build/target/big-data-ai-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8888

# Spark on Java 21 needs these JDK internals opened up
ENTRYPOINT ["java", \
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
    "--add-exports", "java.base/sun.util.calendar=ALL-UNNAMED", \
    "-jar", "app.jar"]
