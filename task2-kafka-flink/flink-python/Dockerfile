FROM flink:1.19.1-scala_2.12-java11

RUN sed -i 's|http://ports.ubuntu.com|https://ports.ubuntu.com|g' /etc/apt/sources.list

RUN for i in 1 2 3 4 5; do \
        apt-get update -o Acquire::Check-Valid-Until=false && \
        apt-get install -y --no-install-recommends python3 python3-dev python3-pip python3-venv openjdk-11-jdk-headless gcc && break || \
        { echo "apt attempt $i failed, retrying..."; sleep 5; }; \
    done && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# apache-flink's pemja dependency builds a native JNI extension requiring JDK
# headers (jni.h); the base image's JAVA_HOME (/opt/java/openjdk) is a JRE
# only, so point at the apt-installed JDK for this build step only.
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac)))) \
    pip3 install --no-cache-dir apache-flink==1.19.2

RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
