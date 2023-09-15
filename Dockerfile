FROM ubuntu:latest as BUILDER

RUN apt update && apt upgrade -y
RUN apt install -y build-essential cmake default-jdk

COPY . /aeron
WORKDIR /aeron

RUN ./cppbuild/cppbuild

FROM alpine:latest

COPY --from=BUILDER cppbuild/Release/binaries /aeron
WORKDIR /aeron

ENTRYPOINT ["./aeronmd"]
