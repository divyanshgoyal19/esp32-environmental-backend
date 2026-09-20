import os
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING, DESCENDING


# ============================================================
# CONFIGURATION
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI")
READ_API_KEY = os.environ.get("THINGSPEAK_READ_API_KEY")

CHANNEL_ID = os.environ.get("THINGSPEAK_CHANNEL_ID", "3499510")

DATABASE_NAME = "environmental_monitoring"
COLLECTION_NAME = "sensor_data"

THINGSPEAK_URL = (
    f"https://api.thingspeak.com/channels/"
    f"{CHANNEL_ID}/feeds/last.json"
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

# Allows your Vercel React frontend to access this API.
CORS(app)


# ============================================================
# MONGODB
# ============================================================

if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is missing.")

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Unique entry_id index was already created in MongoDB.
# Do not recreate it every time the web server starts.



# ============================================================
# DATA CONVERSION
# ============================================================

def safe_float(value):
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def convert_sensor_data(data):

    return {
        "timestamp": data.get("created_at"),
        "entry_id": int(data["entry_id"]),
        "temperature": safe_float(data.get("field1")),
        "humidity": safe_float(data.get("field2")),
        "pressure": safe_float(data.get("field3")),
        "received_at": datetime.now(timezone.utc)
    }


# ============================================================
# THINGSPEAK FETCH
# ============================================================

def fetch_and_store_data():

    if not READ_API_KEY:
        raise RuntimeError(
            "THINGSPEAK_READ_API_KEY environment variable is missing."
        )

    response = requests.get(
        THINGSPEAK_URL,
        params={"api_key": READ_API_KEY},
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    if "entry_id" not in data:
        raise RuntimeError("ThingSpeak returned no entry_id.")

    sensor_data = convert_sensor_data(data)

    collection.update_one(
        {"entry_id": sensor_data["entry_id"]},
        {"$setOnInsert": sensor_data},
        upsert=True
    )

    return sensor_data


# ============================================================
# ROUTES
# ============================================================

@app.route("/", methods=["GET"])
def home():

    return jsonify({
        "status": "success",
        "message": "ESP32 Environmental Monitoring API"
    })


@app.route("/api/health", methods=["GET"])
def health():

    try:
        client.admin.command("ping")

        return jsonify({
            "status": "success",
            "message": "Flask backend and MongoDB are working",
            "database": DATABASE_NAME,
            "collection": COLLECTION_NAME
        })

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/api/latest", methods=["GET"])
def latest():

    try:
        # Get newest reading directly from ThingSpeak
        # and save it in MongoDB.
        fetch_and_store_data()

    except Exception as e:
        # MongoDB data can still be returned if ThingSpeak
        # temporarily fails.
        print("ThingSpeak refresh error:", e)

    latest_record = collection.find_one(
        {},
        {"_id": 0},
        sort=[("entry_id", DESCENDING)]
    )

    if latest_record is None:
        return jsonify({
            "message": "No sensor data available"
        }), 404

    return jsonify(latest_record)


@app.route("/api/readings", methods=["GET"])
def readings():

    records = list(
        collection.find(
            {},
            {"_id": 0}
        )
        .sort("entry_id", DESCENDING)
        .limit(50)
    )

    records.reverse()

    return jsonify(records)


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 5001))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )