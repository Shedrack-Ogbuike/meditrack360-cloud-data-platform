FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies including Java for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages including PySpark
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Hadoop AWS JARs (these are Java libraries, not Python packages)
RUN mkdir -p /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars && \
    wget -q -P /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/ \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    echo "Downloaded Hadoop AWS JARs for S3 support"

# Set environment variables for Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/home/airflow/.local/lib/python3.9/site-packages/pyspark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create Spark configuration directory
RUN mkdir -p $SPARK_HOME/conf