import json
from kafka import KafkaConsumer

# CONFIGURE
consumer = KafkaConsumer(
    "topic_demo",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="grupo-demo",
    consumer_timeout_ms=5000,  # Aguarda 5 segundos por nova mensagem antes de parar
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# CODE
print("Consumindo mensagens...\n")

for message in consumer:
    try:
        evento = message.value
        if "uuid" not in evento or "timestamp" not in evento:
            print(f"Mensagem inválida: {evento}")
            continue
        print(f"""
              Evento recebido:
              - ID: {evento["uuid"]}
              - CPF/CNPJ: {evento["cpf"]}
              - Timestamp: {evento["timestamp"]}
            """)
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

consumer.close()
print("Nenhuma nova mensagem recebida. Encerrando consumidor.")