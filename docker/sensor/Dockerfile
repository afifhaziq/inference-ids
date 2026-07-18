FROM zeek/zeek:6.0.4

ARG LIBRDKAFKA_VERSION=2.3.0
ARG ZEEK_KAFKA_VERSION=v1.2.0
ARG TCPREPLAY_VERSION=4.4.3

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        libssl-dev \
        zlib1g-dev \
        libpcap-dev \
        libnet1-dev \
        wget \
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

# Debian's `apt-get install tcpreplay` ships a build with "Packet editing: disabled"
# (no tcpedit/checksum support), which --fixcsum requires. Build from source instead;
# the same source tree also produces tcpreplay-edit, the packet-editing-enabled binary.
RUN wget -q "https://github.com/appneta/tcpreplay/releases/download/v${TCPREPLAY_VERSION}/tcpreplay-${TCPREPLAY_VERSION}.tar.gz" \
    && tar xzf "tcpreplay-${TCPREPLAY_VERSION}.tar.gz" \
    && cd "tcpreplay-${TCPREPLAY_VERSION}" \
    && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install \
    && cd .. && rm -rf "tcpreplay-${TCPREPLAY_VERSION}" "tcpreplay-${TCPREPLAY_VERSION}.tar.gz"

RUN zkg install --force --version "${ZEEK_KAFKA_VERSION}" https://github.com/SeisoLLC/zeek-kafka

COPY local.zeek /usr/local/zeek/share/zeek/site/local.zeek
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# /pcaps is a read-only bind mount (tcpreplay's input); Zeek needs a writable cwd
# for ad-hoc ASCII-writer debugging (see docs/VERIFICATION.md step 1) even though
# the default Kafka-writer config never writes local log files at all.
WORKDIR /var/log/zeek
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
