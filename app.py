import hmac
import math
import os
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

MONGO_URI = os.environ.get("MONGO_URI")
READ_API_KEY = os.environ.get("THINGSPEAK_READ_API_KEY")
SYNC_SECRET = os.environ.get("SYNC_SECRET")
CHANNEL_ID = os.environ.get("THINGSPEAK_CHANNEL_ID", "3499510")
DATABASE_NAME = "environmental_monitoring"
COLLECTION_NAME = "sensor_data"
THINGSPEAK_FEEDS_URL = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json"
SYNC_WINDOW = timedelta(days=1)
THINGSPEAK_RESULTS_LIMIT = 8000
MAX_PAGE_LIMIT = 200

app = Flask(__name__)
CORS(app)
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is missing.")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
collection = client[DATABASE_NAME][COLLECTION_NAME]


def safe_float(value):
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
    except (AttributeError, ValueError):
        return None


def thingspeak_time(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def convert_sensor_data(feed):
    """Return a valid MongoDB document or None for malformed/empty feeds."""
    try:
        entry_id = int(feed["entry_id"])
    except (KeyError, TypeError, ValueError):
        return None
    timestamp = feed.get("created_at")
    if parse_timestamp(timestamp) is None:
        return None
    temperature = safe_float(feed.get("field1"))
    humidity = safe_float(feed.get("field2"))
    pressure = safe_float(feed.get("field3"))
    if temperature is None and humidity is None and pressure is None:
        return None
    return {
        "entry_id": entry_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "received_at": datetime.now(timezone.utc),
    }


def latest_stored():
    return collection.find_one(
        {}, {"entry_id": 1, "timestamp": 1}, sort=[("entry_id", DESCENDING)]
    )


def fetch_window(start, end):
    if not READ_API_KEY:
        raise RuntimeError("THINGSPEAK_READ_API_KEY environment variable is missing.")
    response = requests.get(
        THINGSPEAK_FEEDS_URL,
        params={
            "api_key": READ_API_KEY,
            "start": thingspeak_time(start),
            "end": thingspeak_time(end),
            "results": THINGSPEAK_RESULTS_LIMIT,
        },
        timeout=20,
    )
    response.raise_for_status()
    feeds = response.json().get("feeds", [])
    if not isinstance(feeds, list):
        raise RuntimeError("ThingSpeak returned an invalid feeds response.")
    return feeds


def channel_start_timestamp():
    """Read the channel creation time for a first-ever MongoDB backfill."""
    if not READ_API_KEY:
        raise RuntimeError("THINGSPEAK_READ_API_KEY environment variable is missing.")
    response = requests.get(
        THINGSPEAK_FEEDS_URL,
        params={"api_key": READ_API_KEY, "results": 1},
        timeout=20,
    )
    response.raise_for_status()
    return parse_timestamp(response.json().get("channel", {}).get("created_at"))


def upsert_feeds(feeds, minimum_entry_id):
    inserted = skipped = 0
    for feed in feeds:
        document = convert_sensor_data(feed)
        if document is None or (
            minimum_entry_id is not None and document["entry_id"] <= minimum_entry_id
        ):
            skipped += 1
            continue
        try:
            result = collection.update_one(
                {"entry_id": document["entry_id"]},
                {"$setOnInsert": document},
                upsert=True,
            )
            inserted += result.upserted_id is not None
        except DuplicateKeyError:
            # Safe if another Render worker/scheduled job inserted this entry first.
            skipped += 1
    return inserted, skipped


def sync_thingspeak_to_mongodb():
    """
    Fetch bounded ThingSpeak time windows and upsert every feed newer than the
    highest MongoDB entry_id. A retry is idempotent because entry_id is unique.
    """
    stored = latest_stored()
    minimum_entry_id = stored["entry_id"] if stored else None
    start = parse_timestamp(stored.get("timestamp")) if stored else None
    start = start or channel_start_timestamp()
    if start is None:
        return {"inserted": 0, "skipped": 0, "latest_entry_id": None}
    now = datetime.now(timezone.utc)
    inserted = skipped = 0

    while start < now:
        end = min(start + SYNC_WINDOW, now)
        batch_inserted, batch_skipped = upsert_feeds(
            fetch_window(start, end), minimum_entry_id
        )
        inserted += batch_inserted
        skipped += batch_skipped
        start = end

    newest = latest_stored()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "latest_entry_id": newest["entry_id"] if newest else None,
    }


# Retained for existing callers; now performs complete bounded synchronization.
def fetch_and_store_data():
    return sync_thingspeak_to_mongodb()


def page_values():
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 50))
    except (TypeError, ValueError):
        return None, None
    return (page, min(limit, MAX_PAGE_LIMIT)) if page > 0 and limit > 0 else (None, None)


def provided_secret():
    return request.headers.get("X-Sync-Secret") or request.headers.get(
        "Authorization", ""
    ).removeprefix("Bearer ").strip()


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "success", "message": "ESP32 Environmental Monitoring API"})


@app.route("/api/health", methods=["GET"])
def health():
    try:
        client.admin.command("ping")
        return jsonify({
            "status": "success",
            "message": "Flask backend and MongoDB are working",
            "database": DATABASE_NAME,
            "collection": COLLECTION_NAME,
        })
    except Exception as error:
        return jsonify({"status": "error", "message": str(error)}), 500


@app.route("/api/latest", methods=["GET"])
def latest():
    try:
        sync_thingspeak_to_mongodb()
    except (requests.RequestException, RuntimeError, PyMongoError) as error:
        # Cached MongoDB data remains useful when ThingSpeak is unavailable.
        app.logger.warning("ThingSpeak synchronization error: %s", error)
    record = collection.find_one({}, {"_id": 0}, sort=[("entry_id", DESCENDING)])
    return jsonify(record) if record else (jsonify({"message": "No sensor data available"}), 404)


@app.route("/api/readings", methods=["GET"])
def readings():
    # Preserve the deployed frontend contract: no query parameters means the
    # original raw-array response, ordered oldest to newest.
    if "page" not in request.args and "limit" not in request.args:
        records = list(
            collection.find({}, {"_id": 0}).sort("entry_id", 1)
        )
        return jsonify(records)

    page, limit = page_values()
    if page is None:
        return jsonify({"status": "error", "message": "page and limit must be positive integers."}), 400
    total_records = collection.count_documents({})
    records = list(
        collection.find({}, {"_id": 0})
        .sort("entry_id", DESCENDING)
        .skip((page - 1) * limit)
        .limit(limit)
    )
    return jsonify({
        "readings": records,
        "pagination": {
            "page": page,
            "limit": limit,
            "total_records": total_records,
            "total_pages": math.ceil(total_records / limit) if total_records else 0,
        },
    })


@app.route("/api/sync", methods=["POST"])
def sync():
    if not SYNC_SECRET:
        return jsonify({"status": "error", "message": "SYNC_SECRET environment variable is not configured."}), 503
    if not hmac.compare_digest(provided_secret(), SYNC_SECRET):
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    try:
        return jsonify({"status": "success", **sync_thingspeak_to_mongodb()})
    except (requests.RequestException, RuntimeError, PyMongoError) as error:
        app.logger.exception("ThingSpeak synchronization failed")
        return jsonify({"status": "error", "message": str(error)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=False)
