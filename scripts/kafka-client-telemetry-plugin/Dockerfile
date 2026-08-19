FROM apache/kafka:3.7.2 AS kafka

FROM eclipse-temurin:17-jdk AS build
COPY --from=kafka /opt/kafka /opt/kafka
COPY src /src

RUN mkdir -p /out \
    && javac -cp "/opt/kafka/libs/*" -d /out \
       /src/com/kafrust/KafrustTelemetryReporter.java \
    && jar cf /out/kafrust-telemetry.jar -C /out com

FROM apache/kafka:3.7.2
COPY --from=build /out/kafrust-telemetry.jar /opt/kafka/libs/kafrust-telemetry.jar
