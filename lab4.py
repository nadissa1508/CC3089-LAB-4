"""
Universidad del Valle de Guatemala
CC3089 - Base de Datos 2
Laboratorio 4 - MongoDB Charts

Descripción:
Este script conecta con MongoDB Atlas, carga dos archivos CSV
(restaurantes y ratings) y realiza inserciones masivas usando Bulk Write.
"""

import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne
import certifi
import ssl

# ==============================
# CARGAR VARIABLES DE ENTORNO
# ==============================

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise Exception("No se encontró MONGO_URI en el archivo .env")

# ==============================
# CONEXIÓN
# ==============================

try:
    # Opción 1: Intentar conexión con certificados de certifi (más seguro)
    print("Intentando conectar a MongoDB Atlas...")
    client = MongoClient(
        MONGO_URI,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=5000
    )
    client.admin.command('ping')
    print("Conexión exitosa a MongoDB Atlas")
except Exception as e:
    print(f"Primera conexión falló: {str(e)[:100]}...")
    print("Intentando método alternativo sin verificación SSL...")
    
    try:
        # Opción 2: Conexión sin verificar certificados (para desarrollo)
        client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsAllowInvalidCertificates=True,
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        print("Conexión exitosa con método alternativo")
    except Exception as e2:
        print(f"Error al conectar con MongoDB: {e2}")
        print("\nSOLUCIONES:")
        print("Verifica tu connection string en .env")
        print("Agrega al final del connection string: &tlsAllowInvalidCertificates=true")
        print("Actualiza paquetes: pip install --upgrade pymongo certifi")
        print("Verifica que tu contraseña esté correctamente codificada (sin <> )")
        exit(1)

db = client["lab_mongodb"]

restaurants_collection = db["restaurants"]
ratings_collection = db["ratings"]

# ==============================
# RESTAURANTES
# ==============================

print("\n--- Cargando Restaurantes ---")
restaurants_df = pd.read_csv("data/geoplaces2.csv")
print(f"Registros leídos: {len(restaurants_df)}")
restaurants_df = restaurants_df.dropna(subset=["placeID", "name"])
print(f"Registros después de limpieza: {len(restaurants_df)}")

restaurants_data = restaurants_df.to_dict("records")
restaurant_ops = [InsertOne(doc) for doc in restaurants_data]

restaurants_collection.delete_many({})
result_restaurants = restaurants_collection.bulk_write(restaurant_ops)

print(f"Restaurantes insertados: {result_restaurants.inserted_count}")

# ==============================
# RATINGS
# ==============================

print("\n--- Cargando Ratings ---")
ratings_df = pd.read_csv("data/rating_final.csv")
print(f"Registros leídos: {len(ratings_df)}")
ratings_df = ratings_df.dropna(subset=["userID", "placeID"])
print(f"Registros después de limpieza: {len(ratings_df)}")

ratings_data = ratings_df.to_dict("records")
rating_ops = [InsertOne(doc) for doc in ratings_data]

ratings_collection.delete_many({})
result_ratings = ratings_collection.bulk_write(rating_ops)

print(f"Ratings insertados: {result_ratings.inserted_count}")

# ==============================
# CERRAR CONEXIÓN
# ==============================

print("\nProceso completado exitosamente")
print(f"Base de datos: {db.name}")
print(f"Colecciones creadas: {db.list_collection_names()}")

client.close()