FROM postgres:14

# Install dependencies required to build decoderbufs
RUN apt-get update
RUN apt-get install -f -y \
      software-properties-common \
      build-essential \
      pkg-config \
      git

RUN apt-get install -f -y \
      postgresql-server-dev-14 \
      libprotobuf-c-dev && \
    rm -rf /var/lib/apt/lists/*

ARG decoderbufs_version=v1.7.0.Final
RUN git clone https://github.com/debezium/postgres-decoderbufs -b $decoderbufs_version --single-branch && \
    cd postgres-decoderbufs && \
    make && make install && \
    cd .. && \
    rm -rf postgres-decoderbufs

COPY 01_init.sql /docker-entrypoint-initdb.d/