FROM nvidia/cuda:10.0-cudnn7-runtime-ubuntu18.04


RUN set -ex; \
    apt-get update; \
    apt-get upgrade -y && \
    apt install openjdk-8-jre  wget  libsnappy1v5 gcc ca-certificates p11-kit -y && \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*;

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.7
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
#ENV PATH $JAVA_HOME/bin:$PATH

RUN set -ex; \
  wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
  wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in $(shuf -e ha.pool.sks-keyservers.net \
                          hkp://p80.pool.sks-keyservers.net:80 \
                          keyserver.ubuntu.com \
                          hkp://keyserver.ubuntu.com:80 \
                          pgp.mit.edu) ; do \
      gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
  done && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true
# Configure Flink version
ENV FLINK_VERSION=1.9.1 \
    HADOOP_SCALA_VARIANT=scala_2.12
# Prepare environment
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink
WORKDIR $FLINK_HOME
ENV FLINK_URL_FILE_PATH=flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-${HADOOP_SCALA_VARIANT}.tgz
# Not all mirrors have the .asc files
ENV FLINK_TGZ_URL=https://archive.apache.org/dist/${FLINK_URL_FILE_PATH} \
    FLINK_ASC_URL=https://archive.apache.org/dist/${FLINK_URL_FILE_PATH}.asc

# For GPG verification instead of relying on key servers
COPY KEYS /KEYS
# Install Flink
RUN set -ex; \
  wget -nv -O flink.tgz "$FLINK_TGZ_URL" --no-check-certificate; \
  tar -xf flink.tgz --strip-components=1; \
  rm flink.tgz; \
  \
  chown -R flink:flink .;

RUN set -ex; \
	{ \
		echo '#!/usr/bin/env bash'; \
		echo 'set -Eeuo pipefail'; \
		echo 'ls $JAVA_HOME'; \
		echo 'cp $JAVA_HOME/jre/lib/ $JAVA_HOME/lib -r'; \
		echo 'if ! [ -d "$JAVA_HOME" ]; then echo >&2 "error: missing JAVA_HOME environment variable"; exit 1; fi'; \
	# 8-jdk uses "$JAVA_HOME/jre/lib/security/cacerts" and 8-jre and 11+ uses "$JAVA_HOME/lib/security/cacerts" directly (no "jre" directory)
		echo 'cacertsFile=; for f in "$JAVA_HOME/lib/security/cacerts" "$JAVA_HOME/jre/lib/security/cacerts"; do if [ -e "$f" ]; then echo "f = $f" && cacertsFile="$f" && echo "cacertsFile=$cacertsFile"; break; fi; done'; \
		echo 'if [ -z "$cacertsFile" ] || ! [ -f "$cacertsFile" ]; then echo >&2 "error: failed to find cacerts file in $JAVA_HOME"; exit 1; fi'; \
		echo 'trust extract --overwrite --format=java-cacerts --filter=ca-anchors --purpose=server-auth "$cacertsFile"'; \
	} > /etc/ca-certificates/update.d/docker-openjdk; \
	chmod +x /etc/ca-certificates/update.d/docker-openjdk; \
	/etc/ca-certificates/update.d/docker-openjdk; \
	# https://github.com/docker-library/openjdk/issues/331#issuecomment-498834472
	find "$JAVA_HOME/lib" -name '*.so' -exec dirname '{}' ';' | sort -u > /etc/ld.so.conf.d/docker-openjdk.conf; \
	ldconfig;

COPY ./libtensorflow_framework.so.1 /usr/lib/x86_64-linux-gnu/

# Add optional libraries into Flink classpath
RUN mkdir -p ${FLINK_HOME}/plugins/s3-fs-presto \
 && mkdir -p ${FLINK_HOME}/plugins/s3-fs-hadoop \
 && ln -fs ${FLINK_HOME}/opt/flink-s3-fs-presto-${FLINK_VERSION}.jar ${FLINK_HOME}/plugins/s3-fs-presto \
 && ln -fs ${FLINK_HOME}/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar ${FLINK_HOME}/plugins/s3-fs-hadoop

# Configure container
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]
