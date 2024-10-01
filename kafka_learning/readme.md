#### create virtual enviroment python
```sh
virtualenv producer-venv
source ./producer-venv/bin/activate
```

#### install the libraries
```sh
pip install -r requirements.txt
```

#### execute application
```sh
/usr/local/bin/python3 app.py
```

docker exec -it <nome_container> bash
    docker exec -it cfcafc36cee7 bash
kafka-topics --create --bootstrap-server localhost:19092 --replication-factor 3 --partitions 3 --topic meutopico