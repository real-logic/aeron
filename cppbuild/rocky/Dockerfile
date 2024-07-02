ARG VERSION="8"
FROM rockylinux:${VERSION} as builder

ARG USER_ID="1000"
ARG GROUP_ID="1000"
ARG GCC_VERSION="10"

RUN groupadd --gid $GROUP_ID --non-unique --system athena
RUN adduser --uid $USER_ID --system --gid $GROUP_ID athena
RUN mkdir -p /home/athena && \
    printf "PS1=\"Rocky> \"" > /home/athena/.bashrc && \
    chown -R athena /home/athena

# Install dev tools
RUN yum install -y drpm scl-utils && \
    yum update -y && \
    yum install -y "gcc-toolset-${GCC_VERSION}-gcc" "gcc-toolset-${GCC_VERSION}-gcc-c++" make findutils git libuuid-devel zlib-devel openssl-devel

# Install Java
RUN yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum install --disableplugin=subscription-manager -y zulu-8

ENV JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_VERSION=8 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail" \
    GCC_VERSION=${GCC_VERSION}

USER athena
WORKDIR /opt/aeron

FROM builder as essentials-entrypoint
ENTRYPOINT scl enable "gcc-toolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package

FROM builder as extended-builder

ADD --chown=athena:athena . /opt/aeron

RUN git config --global user.email "build@aeron.io"
RUN git config --global user.name "Athena (Build Master)"
RUN git config --global core.autocrlf false
RUN git update-index -q --refresh

FROM extended-builder as essentials-build
RUN scl enable "gcc-toolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package

FROM scratch AS essentials-artifacts
COPY --from=essentials-build /opt/aeron/cppbuild/Release/*.tar.gz /