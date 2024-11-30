FROM quay.io/astronomer/astro-runtime:12.4.0
RUN mkdir -p /usr/local/airflow/keys
# Copiar o arquivo JSON para o container
COPY ./keys/engdados-42c32eb1ca20.json /usr/local/airflow/keys/gcp-key.json
# ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/keys/gcp-key.json

