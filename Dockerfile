FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME for Intel CPU
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
# Or for ARM CPU
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy and install Python dependencies first (this will be cached if requirements.txt doesn't change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user
USER astro
