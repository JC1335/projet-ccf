# Image basée sur Debian avec Java
FROM openjdk:17-jdk-slim

# Installer wget
RUN apt-get update && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

# Variables d’environnement
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

# Télécharger et installer Spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar xvf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C / \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Variables Spark
ENV SPARK_HOME=/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Dossier de travail
WORKDIR /app

# Copier le .jar compilé
COPY target/scala-2.12/project_2.12-0.1.0-SNAPSHOT.jar .

# Copier les données (si le dossier existe)
COPY ./data /app/data

# Lancer Spark avec le jar
ENTRYPOINT ["spark-submit", \
            "--class", "CCFDataFrame", \
            "--master", "spark://spark-master:7077", \
            "project_2.12-0.1.0-SNAPSHOT.jar"]
