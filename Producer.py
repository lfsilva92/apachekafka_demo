import uuid
import json
import time
import pytz
import random
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker

# CONFIGURE
fake = Faker('pt_BR')
tz = pytz.timezone('America/Sao_Paulo')

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3
)

# FUNCTIONS
def gerar_cpf():
    return fake.cpf().replace('.', '').replace('-', '')

def gerar_nup():
    return ''.join([str(random.randint(0, 9)) for _ in range(14)])

def arquivo_json():
    return{
        "uuid":str(uuid.uuid4()),
        "cpf":gerar_cpf(),
        "timestamp": datetime.now(tz).isoformat()
    }
    
# CODE
for i in range(5):
    evento = arquivo_json()
    
    producer.send(
        "topic_demo", # Nome do tópico do Kafka
        key=evento["uuid"].encode("utf-8"), # Particionamento pelo UUID
        value=evento
    )
    print(f"Enviado: {evento}")
    time.sleep(3)
    
producer.flush()
producer.close()