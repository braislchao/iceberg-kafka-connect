# Java 17 or later: Iceberg 1.12 is Java 17 bytecode and Connect skips it silently on Java 11
ARG CONNECT_VERSION=7.7.1
FROM confluentinc/cp-kafka-connect:${CONNECT_VERSION}
COPY plugin/ /usr/share/confluent-hub-components/
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.7.0
