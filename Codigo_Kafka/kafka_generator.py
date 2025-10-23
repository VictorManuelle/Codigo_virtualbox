from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

def create_kafka_producer():
    """Crear productor Kafka"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def generate_realtime_gas_data():
    """Generar datos simulados en tiempo real"""
    empresas = ["EMGAS S.A. ESP", "GAS NATURAL S.A.", "GASES DE ANTIOQUIA S.A.", 
                "GAS CARIBE S.A.", "PROMIGAS S.A. ESP"]
    municipios = ["BOGOTA D.C.", "MEDELLIN", "CALI", "BARRANQUILLA", "CARTAGENA"]
    estratos = ["1", "2", "3", "4", "5", "6", "COMERCIAL", "INDUSTRIAL"]
    
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "empresa": random.choice(empresas),
        "municipio": random.choice(municipios),
        "estrato": random.choice(estratos),
        "cargo_fijo": round(random.uniform(10000, 50000), 2),
        "rango_0": round(random.uniform(800, 1500), 2),
        "rango_21": round(random.uniform(700, 1300), 2),
        "consumo_simulado": random.randint(20, 100)
    }

def start_data_generator(topic="gas-tarifas-stream"):
    """Iniciar generador de datos en tiempo real"""
    producer = create_kafka_producer()
    
    print(" Iniciando generador de datos en tiempo real...")
    print(" Enviando datos al topic: gas-tarifas-stream")
    print("  Presiona Ctrl+C para detener\n")
    
    try:
        counter = 0
        while True:
            data = generate_realtime_gas_data()
            producer.send(topic, value=data)
            
            counter += 1
            print(f"[{counter}] {data['empresa']} - {data['municipio']} - "
                  f"Estrato {data['estrato']} - Cargo: ${data['cargo_fijo']:,.0f}")
            
            time.sleep(2)  # Generar dato cada 2 segundos
            
    except KeyboardInterrupt:
        print("\n Deteniendo generador de datos...")
    finally:
        producer.close()

# Si este archivo es el programa principal (el que se ejecuto directamente), entonces enciende el generador de datos
if __name__ == "__main__":
    start_data_generator()
