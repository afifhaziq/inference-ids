FROM eclipse-temurin:17-jre-jammy

ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13

RUN apt-get update && apt-get install -y --no-install-recommends \
        wget \
        netcat-openbsd \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && tar xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt \
    && mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

ENV PATH="/opt/kafka/bin:/root/.local/bin:${PATH}"
RUN wget -qO- https://astral.sh/uv/install.sh | sh

WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN uv python install 3.12 && uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY config ./config
COPY docker/backend/server.properties /opt/kafka/config/kraft/server.properties
COPY docker/backend/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV PYTHONPATH=/app/src

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
