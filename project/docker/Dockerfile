# Base image avec Java (Debian-based)
FROM openjdk:17-jdk-slim

# Installer wget et ps (utile pour Spark)
RUN apt-get update && apt-get install -y wget procps \
    && rm -rf /var/lib/apt/lists/*

# Variables Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

# Télécharger et installer Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C / \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Définir les variables d'environnement Spark
ENV SPARK_HOME=/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Dossier de travail
WORKDIR /app

# Copier le JAR compilé (✅ corrigé ici)
COPY target/scala-2.12/graph_scala_2.12-1.0.jar .

# Copier les données si nécessaires
COPY ./data /app/data

# Lancer l'application Spark
ENTRYPOINT ["spark-submit", \
            "--class", "CCFDataFrame", \
            "--master", "spark://spark-master:7077", \
            "graph_scala_2.12-1.0.jar"]
