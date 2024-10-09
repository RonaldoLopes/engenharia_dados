source /home/ronaldo/engenharia_dados/spark/series-spark/pyvenv/bin/activate

# Instale ou reinstale as dependências necessárias
pip uninstall -y pyspark findspark
pip install pyspark==3.4.1 findspark

# Configure as variáveis de ambiente dentro do virtualenv
export JAVA_HOME=/usr/bin/java  # Ajuste para seu caminho do Java wich java para pegar o caminho
export SPARK_HOME=/home/ronaldo/engenharia_dados/spark/series-spark/pyvenv/bin/spark-submit  # Ajuste para seu caminho do Spark pegar caminho which spark-submit
pip freeze | grep pyspark

export PYSPARK_PYTHON=$VIRTUAL_ENV/bin/python
export PYSPARK_DRIVER_PYTHON=$VIRTUAL_ENV/bin/python
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

# Verifique as configurações
echo "Python path: $(which python)"
echo "Java version: $(java -version 2>&1 | head -n 1)"
echo "Spark home: $SPARK_HOME"

# Usar o findspark:
Principais funções do findspark:
Localização automática do Spark
Encontra automaticamente a instalação do Spark no seu sistema
Evita a necessidade de configurar manualmente o SPARK_HOME
Configuração do PYTHONPATH
Adiciona os caminhos necessários do Spark ao PYTHONPATH
Permite que o Python encontre os módulos do PySpark
Inicialização simplificada
Configura todas as variáveis de ambiente necessárias
Torna o processo de inicialização do Spark mais fácil e consistente