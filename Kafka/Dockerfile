FROM confluentinc/cp-kafka:7.0.1
WORKDIR /app
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]