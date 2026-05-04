FROM spark:3.5.4-java17-python3

USER root

# Install Python Dependencies
COPY ./spark/requirements.txt /opt/
RUN pip install -r /opt/requirements.txt

# Install required jars
#RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.4/spark-connect_2.12-3.5.4.jar -P /opt/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.4/spark-connect_2.12-3.5.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/spark/jars/ && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -P /opt/spark/jars/

USER spark