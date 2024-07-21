ARG VERSION="9.3"
FROM rockylinux:${VERSION} as builder

ARG GCC_VERSION="13"

RUN yum update -y && yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum update -y && yum install -y \
    scl-utils \
    gcc-toolset-${GCC_VERSION}-gcc \
    gcc-toolset-${GCC_VERSION}-gcc-c++ \
    make \
    tar \
    zlib-devel \
    libuuid-devel \
    git \
    findutils \
    openssl-devel \
    zulu-8

ENV JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_VERSION=8 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail" \
    GCC_VERSION=${GCC_VERSION}

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
RUN scl enable "gcc-toolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package

FROM scratch AS essentials-artifacts
COPY --from=essentials-build /opt/aeron/cppbuild/Release/*.tar.gz /