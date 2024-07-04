ARG VERSION="2023"
FROM amazonlinux:${VERSION} as builder

RUN yum update && yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum update && yum install -y \
    gcc \
    g++ \
    make \
    tar \
    zlib-devel \
    libbsd-devel \
    libuuid-devel \
    git \
    zulu-8 \
    doxygen \
    python3

ENV JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_VERSION=8 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail"

ARG USER_ID="1000"
ARG GROUP_ID="1000"
RUN groupadd --gid $GROUP_ID --non-unique --system athena
RUN adduser --uid $USER_ID --system --create-home --gid $GROUP_ID athena

USER athena
WORKDIR /opt/aeron
ADD --chown=athena:athena . /opt/aeron

RUN git config --global user.email "build@aeron.io"
RUN git config --global user.name "Athena (Build Master)"
RUN git config --global core.autocrlf false
RUN git update-index -q --refresh

FROM builder as essentials-build
RUN cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package

FROM scratch AS essentials-artifacts
COPY --from=essentials-build /opt/aeron/cppbuild/Release/*.tar.gz /