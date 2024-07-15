import time
import re
import datetime
from kafka import KafkaProducer as kp

arquivo = open(r'/var/log/apache2/access.log','r')
regexp = r'GET / HTTP/1.1'
produtor = kp(bootstrap_servers="127.0.0.1:9092")

while True:
	linha = arquivo.readline()
	if not linha:
		time.sleep(5)
	else:
		x = re.findall(regexp, linha, re.MULTILINE)
		if x:
			for match in x:
				msg = bytes(str(match), encoding='ascii')
				produtor.send("apachelog", msg)
				produtor.flush()
				print("Mensagem enviada em ", datetime.datetime.now())
		else:
			print("Linha não corresponde ao padrão:", linha)
