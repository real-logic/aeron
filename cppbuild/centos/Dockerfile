FROM centos:7

ARG USER_ID
ARG GROUP_ID
ARG CMAKE_VERSION=3.14.7
ARG GCC_VERSION

RUN groupadd --gid $GROUP_ID --system athena
RUN adduser --uid $USER_ID --system --gid $GROUP_ID athena

# Install dev tools
RUN yum install -y centos-release-scl && \
    yum install -y "devtoolset-${GCC_VERSION}-gcc" "devtoolset-${GCC_VERSION}-gcc-c++" make

# Install Java
RUN rpm --import http://repos.azulsystems.com/RPM-GPG-KEY-azulsystems && \
    curl -o /etc/yum.repos.d/zulu.repo http://repos.azulsystems.com/rhel/zulu.repo && \
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
ENTRYPOINT scl enable "devtoolset-${GCC_VERSION}" -- cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package
