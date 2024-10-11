# Engenharia de dados
## Repositorio com diversos dados contendo estudos e dia a dia sobre engenharia de dados.
## Notebooks e URLS Databricks(válidos por 6 meses)
### DataFrame1_basico.ipynb
[DataFrame1_basico.ipynb](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/2583795369382292/7836143825856882/latest.html)
### importa_dados.ipynb
[importa_dados.ipynb](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/355038990909768/7836143825856882/latest.html)
### Deltalake1.ipynb
[Deltalake1.ipynb](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/2744160295569945/7836143825856882/latest.html)
### DeltaLake_Upsert.ipynb
[DeltaLake_Upsert.ipynb](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/1893870208292263/7836143825856882/latest.html)
### DeltaLake3_mergeschema.ipynb
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/615613011029692/7836143825856882/latest.html
### Partition.ipynb Particionamento de tabelas
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/782778290556145/7836143825856882/latest.html
### Dataviz.ipynb graficos com databricks
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/782778290556156/7836143825856882/latest.html
### DashBoard Simples
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/907645525451663/782778290556156/7836143825856882/latest.html

### kafka - exemplos simples

# kafka_logs exemplo de leitura de logs com apache e kafka
## Instalação e configuração Apache
* sudo apt update
* sudo apt install apache2
 * sudo systemctl status apache2
* Ajuste Firewall (opcional)
 * sudo ufw allow 'Apache'
* criar um arquivo de log(access.log) -> /var/log/apache2
 * cat access.log

## kafka
* ./kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic apachelog --create --partitions 3 --replication-factor 1
* ./kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic apachelog

### Airflow
## airflow-project com astro
* astro dev start/astro dev stop
## airflow_data com docker_compose