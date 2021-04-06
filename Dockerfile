# Just a simple dockerfile for SAM
FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y install \
    libboost-all-dev \
    cmake \
    clang \
    gcc \
    libprotobuf-dev \
    protobuf-compiler \
    default-jdk \
    scala \
    libzmq3-dev \
    -V


COPY . /opt/SAM/
WORKDIR /opt/SAM/
RUN mkdir -p build && \
    cd build && \
    cmake .. && \
    make

