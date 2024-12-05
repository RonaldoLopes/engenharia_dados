FROM quay.io/astronomer/astro-runtime:12.4.0

RUN mkdir -p /usr/local/airflow/keys

# Use --chmod ao copiar
COPY --chmod=644 ./keys/engdados-7a76a02748bb.json /usr/local/airflow/keys/gcp-key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/keys/gcp-key.json

FROM quay.io/astronomer/astro-runtime:12.4.0

RUN mkdir -p /usr/local/airflow/keys

COPY keys/engdados-7a76a02748bb.json /usr/local/airflow/keys/gcp-key.json

RUN ls -l /usr/local/airflow/keys/
RUN cat /usr/local/airflow/keys/gcp-key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/keys/gcp-key.json

