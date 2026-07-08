FROM ubuntu:22.04

ARG SRT_TAG=v1.5.5
ARG RELAY_VERSION=dev

RUN apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
        build-essential \
        ca-certificates \
        cmake \
        g++ \
        git \
        libssl-dev \
        pkg-config \
        tclsh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/srt
RUN git clone --branch "${SRT_TAG}" --depth 1 https://github.com/Haivision/srt.git .
RUN ./configure --prefix=/usr/local --enable-apps=OFF --enable-bonding \
    && make -j"$(nproc)" \
    && make install \
    && ldconfig

WORKDIR /src/app
ARG RELAY_SOURCE_SHA=dev
RUN printf '%s\n' "$RELAY_SOURCE_SHA" > /tmp/relay-source.sha
COPY src/ ./src/
RUN g++ -O2 -DRELAY_VERSION=\"${RELAY_VERSION}\" -o srt-bonding-relay src/*.c \
    $(pkg-config --cflags --libs srt) -lpthread -lssl -lcrypto -lm

RUN set -eux; \
    stage=/package; \
    mkdir -p "$stage/bin" "$stage/lib"; \
    install -m 755 srt-bonding-relay "$stage/bin/srt-bonding-relay"; \
    ldd "$stage/bin/srt-bonding-relay" | awk '/=> \// {print $3}' | while read -r lib; do \
        case "$lib" in \
            /lib/*/libc.so.*|/lib/*/libpthread.so.*|/lib/*/libm.so.*|/lib/*/libdl.so.*|/lib/*/ld-linux-*.so.*) ;; \
            *) cp -v "$lib" "$stage/lib/" ;; \
        esac; \
    done
