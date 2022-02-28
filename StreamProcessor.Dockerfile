FROM openjdk:8

COPY . .

RUN chmod +x ./gradlew

ENTRYPOINT ["./gradlew", "runStreamProcessor"]