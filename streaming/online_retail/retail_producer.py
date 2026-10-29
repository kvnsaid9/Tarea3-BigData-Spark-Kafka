import csv
import json
import time
from kafka import KafkaProducer

# Productor de Kafka: reproduce las ventas de Online Retail II como si ocurrieran en vivo
RUTA_CSV = "/home/vboxuser/online_retail_II.csv"
TOPIC = "ventas_retail"
PAUSA = 0.1  # segundos entre ventas (unas 10 ventas por segundo)


def ventas_validas(ruta):
    # Aplica la misma limpieza del analisis batch
    with open(ruta, newline="", encoding="utf-8", errors="replace") as archivo:
        for fila in csv.DictReader(archivo):
            try:
                cantidad = int(fila["Quantity"])
                precio = float(fila["Price"])
            except (ValueError, KeyError):
                continue
            descripcion = (fila["Description"] or "").strip().upper()
            if fila["Invoice"].startswith("C") or cantidad <= 0 or precio <= 0 or not descripcion:
                continue
            yield {
                "invoice": fila["Invoice"],
                "description": descripcion,
                "quantity": cantidad,
                "price": precio,
                "total": round(cantidad * precio, 2),
                "country": fila["Country"],
                "customer_id": fila["Customer ID"] or None,
                "original_date": fila["InvoiceDate"],
            }


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for venta in ventas_validas(RUTA_CSV):
    venta["timestamp"] = int(time.time())  # hora de llegada del evento
    producer.send(TOPIC, value=venta)
    print(f"Sent: {venta['invoice']} | {venta['country']} | {venta['description'][:28]} | "
          f"{venta['quantity']} x {venta['price']} = GBP {venta['total']}")
    time.sleep(PAUSA)

producer.flush()
