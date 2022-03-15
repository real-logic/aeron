FROM registry.access.redhat.com/ubi8

ARG USER_ID
ARG GROUP_ID

RUN groupadd --gid $GROUP_ID --system athena
RUN adduser --uid $USER_ID --system --gid $GROUP_ID athena

# Install dev tools
RUN yum install --disableplugin=subscription-manager -y gcc gcc-c++ cmake make

# Install Java
RUN rpm --import http://repos.azulsystems.com/RPM-GPG-KEY-azulsystems && \
    curl -o /etc/yum.repos.d/zulu.repo http://repos.azulsystems.com/rhel/zulu.repo && \
    yum install --disableplugin=subscription-manager -y zulu-8

ENV JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_HOME=/usr/lib/jvm/zulu-8 \
    BUILD_JAVA_VERSION=8 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail" \
    CC=gcc \
    CXX=g++

USER athena

WORKDIR /opt/aeron
ENTRYPOINT ["cppbuild/cppbuild", "--c-warnings-as-errors", "--cxx-warnings-as-errors", "--package"]
