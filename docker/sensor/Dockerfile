FROM zeek/zeek:6.0.4

ARG LIBRDKAFKA_VERSION=2.3.0
ARG ZEEK_KAFKA_VERSION=v1.3.0

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        libssl-dev \
        zlib1g-dev \
        wget \
        tcpreplay \
        iproute2 \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz" \
    && tar xzf "v${LIBRDKAFKA_VERSION}.tar.gz" \
    && cd "librdkafka-${LIBRDKAFKA_VERSION}" \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && ldconfig \
    && cd .. && rm -rf "librdkafka-${LIBRDKAFKA_VERSION}" "v${LIBRDKAFKA_VERSION}.tar.gz"

ENV LIBRDKAFKA_ROOT=/usr/local

RUN zkg install --force "seisollc/zeek-kafka@${ZEEK_KAFKA_VERSION}"

COPY local.zeek /usr/local/zeek/share/zeek/site/local.zeek
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /pcaps
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
