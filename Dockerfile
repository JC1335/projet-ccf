FROM openjdk:17-jdk-slim

# Copier Spark depuis le disque local (tu dois déjà avoir téléchargé et décompressé le tgz)
COPY spark-3.5.0-bin-hadoop3 /spark

ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /app

# Copier le JAR Scala compilé
COPY target/scala-2.12/graph_scala_2.12-1.0.jar .

# Copier les données si présentes
COPY ./data /app/data

ENTRYPOINT ["spark-submit", "--class", "CCFDataFrame", "--master", "local[*]", "graph_scala_2.12-1.0.jar"]
