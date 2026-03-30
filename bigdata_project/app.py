from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

client = MongoClient("mongodb://localhost:27017/")
db = client["twitter_db"]


@app.route('/')
def index():
    return render_template('index.html')


# ── existing: top 10 trends ──────────────────────────────────
@app.route('/api/trends')
def get_trends():
    trends = list(
        db.trends.find({}, {'_id': 0})
        .sort('count', -1)
        .limit(10)
    )
    return jsonify(trends)


# ── analytics summary (counts, stats, top/bottom 5) ─────────
@app.route('/api/analytics')
def get_analytics():
    summary = db.analytics_summary.find_one({"_id": "summary"}, {"_id": 0})
    if not summary:
        return jsonify({})
    if "updated_at" in summary:
        summary["updated_at"] = str(summary["updated_at"])
    return jsonify(summary)


# ── velocity: fastest rising hashtags ───────────────────────
@app.route('/api/velocity')
def get_velocity():
    doc = db.velocity_alerts.find_one({"_id": "latest"}, {"_id": 0})
    if not doc:
        return jsonify({"data": []})
    if "updated_at" in doc:
        doc["updated_at"] = str(doc["updated_at"])
    return jsonify(doc)


# ── peak hours: 24-hour activity chart ──────────────────────
@app.route('/api/peak-hours')
def get_peak_hours():
    doc = db.peak_hours.find_one({"_id": "summary"}, {"_id": 0})
    if not doc:
        return jsonify({})
    if "updated_at" in doc:
        doc["updated_at"] = str(doc["updated_at"])
    return jsonify(doc)


# ── co-occurrence: top hashtag pairs ────────────────────────
@app.route('/api/cooccurrence')
def get_cooccurrence():
    doc = db.cooccurrence_summary.find_one({"_id": "top10"}, {"_id": 0})
    if not doc:
        return jsonify({"pairs": []})
    if "updated_at" in doc:
        doc["updated_at"] = str(doc["updated_at"])
    return jsonify(doc)


# ── all hashtags full list (for table) ──────────────────────
@app.route('/api/all-hashtags')
def get_all_hashtags():
    tags = list(
        db.trends.find({}, {'_id': 0, 'word': 1, 'count': 1, 'rank': 1, 'last_seen': 1})
        .sort('count', -1)
    )
    for t in tags:
        if 'last_seen' in t:
            t['last_seen'] = str(t['last_seen'])
    return jsonify(tags)


if __name__ == '__main__':
    app.run(debug=True, port=5001)

