FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
# For Windows OS
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
# For MAC OS
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

RUN pip install -U pip
RUN pip install --no-cache-dir minio

# COPY jars.txt /tmp/jars.txt
# RUN mkdir -p /opt/spark/jars && \
#     wget -i /tmp/jars.txt -P /opt/spark/jars/
