# ClusterFuzzLite build image (OSS-Fuzz conventions). The base image brings
# the JDK (17), Jazzer, and the fuzzing toolchain; build.sh does the rest.
FROM gcr.io/oss-fuzz-base/base-builder-jvm

# The base image ships no Maven; a pinned binary distribution from the Apache
# archive keeps the fuzz build reproducible.
ENV MAVEN_VERSION=3.9.9
RUN curl -fsSL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    | tar -xz -C /opt \
    && ln -s "/opt/apache-maven-${MAVEN_VERSION}/bin/mvn" /usr/local/bin/mvn

COPY . $SRC/tabellarium
COPY .clusterfuzzlite/build.sh $SRC/build.sh
WORKDIR $SRC/tabellarium
