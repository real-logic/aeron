FROM rockylinux:8

ARG USER_ID
ARG GROUP_ID
ARG CMAKE_VERSION=3.14.7
ARG GCC_VERSION

RUN groupadd --gid $GROUP_ID --non-unique --system athena
RUN adduser --uid $USER_ID --system --gid $GROUP_ID athena

# Install dev tools
RUN yum install -y scl-utils && \
    yum install -y "gcc-toolset-${GCC_VERSION}-gcc" "gcc-toolset-${GCC_VERSION}-gcc-c++" make findutils git

# Install Java
RUN yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum install --disableplugin=subscription-manager -y zulu-8

# Install CMake
WORKDIR /opt/tools
RUN curl -o cmake.tar.gz -L https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz && \
    tar xf cmake.tar.gz

ENV PATH="$PATH:/opt/tools/cmake-${CMAKE_VERSION}-Linux-x86_64/bin" \
    JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_VERSION=8 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail" \
    GCC_VERSION=${GCC_VERSION}

USER athena

WORKDIR /opt/aeron
ENTRYPOINT scl enable "gcc-toolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package
