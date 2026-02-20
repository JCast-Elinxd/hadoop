FROM ubuntu

RUN apt-get update && apt-get install -y vim python3 openjdk-8-jdk net-tools dnsutils wget python3-pip python3-venv

# Descargar y descomprimir Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz && \
    tar -xvzf hadoop-3.4.0.tar.gz && mv hadoop-3.4.0 /opt && cd /opt && ln -s hadoop-3.4.0 hadoop && \
    rm /hadoop-3.4.0.tar.gz

# Preparar directorios HDFS
RUN mkdir -p /opt/hdfs/namenode && mkdir -p /opt/hdfs/datanode

# Crear y activar un entorno virtual
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Instalar mrjob en el entorno virtual
RUN pip install mrjob
RUN pip install setuptools

# Copiar configuración de Hadoop
COPY config/* /opt/hadoop-3.4.0/etc/hadoop/

# Configurar variables de entorno de Hadoop
ENV HADOOP_HOME=/opt/hadoop-3.4.0
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar
ENV PATH=$PATH:$HADOOP_HOME
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
