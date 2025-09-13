FROM eclipse-temurin:17-jre
WORKDIR /app
COPY build/libs/producer-app-all.jar app.jar

LABEL authors="jo-eunji"

ENTRYPOINT ["sh","-c","java -jar /app/app.jar"]