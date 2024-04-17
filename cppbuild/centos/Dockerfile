FROM centos:7

ARG USER_ID
ARG GROUP_ID
ARG GCC_VERSION

RUN groupadd --gid $GROUP_ID --non-unique --system athena
RUN adduser --uid $USER_ID --system --gid $GROUP_ID athena
RUN mkdir -p /home/athena && \
    printf "PS1=\"CentOS> \"" > /home/athena/.bashrc && \
    chown -R athena /home/athena

# Install dev tools
RUN yum install -y centos-release-scl && \
    yum install -y "devtoolset-${GCC_VERSION}-gcc" "devtoolset-${GCC_VERSION}-gcc-c++" make git libuuid-devel zlib-devel openssl11

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
ENTRYPOINT scl enable "devtoolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package
