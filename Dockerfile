# Multi-stage: build with Maven, run on a slim JRE.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/redrive-*.jar app.jar
COPY --from=build /app/src/main/resources/certs/kafka-truststore.p12 /app/certs/kafka-truststore.p12
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
