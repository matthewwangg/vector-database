FROM ubuntu:24.04 AS builder

WORKDIR /vector-database

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    libgrpc++-dev \
    libprotoc-dev \
    git

COPY . .

RUN protoc --proto_path=engine/proto \
    --cpp_out=engine/proto \
    --grpc_out=engine/proto \
    --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin \
    --experimental_allow_proto3_optional \
    engine/proto/*.proto && \
    protoc --proto_path=server/proto \
        --cpp_out=server/proto \
        --grpc_out=server/proto \
        --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin \
        server/proto/*.proto

RUN cmake -S . -B build && cmake --build build -j

FROM ubuntu:24.04

WORKDIR /vector-database

RUN apt-get update && apt-get install -y \
    libprotobuf-dev \
    libgrpc++-dev \
    libprotoc-dev

COPY --from=builder /vector-database/build/server/vector_db_server .

ENTRYPOINT ["./vector_db_server"]
