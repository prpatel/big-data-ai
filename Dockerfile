FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY target/big-data-ai-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8888

# Spark on Java 21 needs these JDK internals opened up
ENTRYPOINT ["java", \
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
    "--add-exports", "java.base/sun.util.calendar=ALL-UNNAMED", \
    "-jar", "app.jar"]
