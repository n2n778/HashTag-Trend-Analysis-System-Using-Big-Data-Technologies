from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, lower, regexp_extract,
    window, desc, asc, current_timestamp, count,
    hour, from_unixtime, unix_timestamp,
    collect_list, size, array_distinct,
    stddev, avg, percentile_approx, max as spark_max, min as spark_min
)
from pymongo import MongoClient
import datetime

spark = SparkSession.builder \
    .appName("TwitterTrendingAnalytics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ═══════════════════════════════════════════════════════════════
# 1. INGEST — read raw tweets from Kafka
# ═══════════════════════════════════════════════════════════════
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_stream") \
    .option("startingOffsets", "latest") \
    .load()

# ═══════════════════════════════════════════════════════════════
# 2. FILTER — keep only hashtag tokens
# ═══════════════════════════════════════════════════════════════
words_df = raw_df \
    .selectExpr("CAST(value AS STRING) as text") \
    .select(
        col("text"),
        explode(split(col("text"), " ")).alias("word"),
        current_timestamp().alias("event_time")
    ) \
    .filter(col("word").startswith("#")) \
    .filter(col("word").rlike("^#[A-Za-z0-9_]{2,}$"))

# ═══════════════════════════════════════════════════════════════
# 3. TRANSFORM — normalize hashtags
# ═══════════════════════════════════════════════════════════════
cleaned_df = words_df \
    .withColumn("word", lower(col("word"))) \
    .withColumn("word", regexp_extract(col("word"), r"(#[a-z0-9_]+)", 1)) \
    .filter(col("word") != "")

# ═══════════════════════════════════════════════════════════════
# 4a. AGGREGATE — global counts (for main trends + dashboard)
# ═══════════════════════════════════════════════════════════════
global_counts = cleaned_df \
    .groupBy("word") \
    .count()

# ═══════════════════════════════════════════════════════════════
# 4b. AGGREGATE — windowed counts (60s window, 30s slide)
#     → Velocity / fastest rising detection
# ═══════════════════════════════════════════════════════════════
window_counts = cleaned_df \
    .groupBy(
        window(col("event_time"), "60 seconds", "30 seconds"),
        col("word")
    ) \
    .count()

# ═══════════════════════════════════════════════════════════════
# 4c. AGGREGATE — hourly counts (peak hour detection)
# ═══════════════════════════════════════════════════════════════
hourly_counts = cleaned_df \
    .withColumn("hour_bucket", hour(col("event_time"))) \
    .groupBy("hour_bucket", "word") \
    .count()

# ═══════════════════════════════════════════════════════════════
# 4d. AGGREGATE — co-occurrence per tweet
#     Collect all hashtags per tweet text, then pair them
# ═══════════════════════════════════════════════════════════════
cooccur_df = cleaned_df \
    .groupBy("text") \
    .agg(collect_list("word").alias("tags")) \
    .filter(size(col("tags")) > 1)


# ═══════════════════════════════════════════════════════════════
# HELPER — get MongoDB connection
# ═══════════════════════════════════════════════════════════════
def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client, client["twitter_db"]


# ═══════════════════════════════════════════════════════════════
# OUTPUT A — Main trends + full analytics summary
#            Writes to: trends, analytics_summary
# ═══════════════════════════════════════════════════════════════
def save_analytics(batch_df, batch_id):
    try:
        ranked = batch_df.orderBy(desc("count"))
        rows   = ranked.collect()
        if not rows:
            print(f"[Batch {batch_id}] empty"); return

        client, db = get_db()
        now = datetime.datetime.utcnow()

        # ── per-hashtag upsert with rank ──────────────────────
        for rank, row in enumerate(rows, start=1):
            db.trends.update_one(
                {"word": row["word"]},
                {
                    "$inc": {"count": int(row["count"])},
                    "$set": {"rank": rank, "last_seen": now}
                },
                upsert=True
            )

        # ── analytics_summary document ────────────────────────
        all_docs  = list(db.trends.find({}, {"_id": 0, "word": 1, "count": 1}))
        counts    = [d["count"] for d in all_docs]
        total     = sum(counts)
        unique    = len(counts)
        avg_c     = round(total / unique, 2) if unique else 0

        # standard deviation (manual, no numpy needed)
        if unique > 1:
            mean   = total / unique
            variance = sum((c - mean) ** 2 for c in counts) / unique
            std_dev  = round(variance ** 0.5, 2)
        else:
            std_dev = 0

        # median
        sorted_counts = sorted(counts)
        mid    = unique // 2
        median = sorted_counts[mid] if unique % 2 else (sorted_counts[mid-1] + sorted_counts[mid]) / 2

        # percentiles (p25, p75)
        p25 = sorted_counts[max(0, int(unique * 0.25) - 1)] if unique else 0
        p75 = sorted_counts[max(0, int(unique * 0.75) - 1)] if unique else 0

        most_pop  = max(all_docs, key=lambda x: x["count"]) if all_docs else {}
        least_pop = min(all_docs, key=lambda x: x["count"]) if all_docs else {}
        top5      = sorted(all_docs, key=lambda x: x["count"], reverse=True)[:5]
        bottom5   = sorted(all_docs, key=lambda x: x["count"])[:5]

        db.analytics_summary.replace_one(
            {"_id": "summary"},
            {
                "_id": "summary",
                "total_mentions":  total,
                "unique_hashtags": unique,
                "avg_count":       avg_c,
                "std_dev":         std_dev,
                "median":          median,
                "p25":             p25,
                "p75":             p75,
                "most_popular":    most_pop,
                "least_popular":   least_pop,
                "top5":            top5,
                "bottom5":         bottom5,
                "updated_at":      now
            },
            upsert=True
        )

        client.close()
        print(f"[Batch {batch_id}] {unique} tags | total={total} | top={most_pop.get('word','?')} | stddev={std_dev}")

    except Exception as e:
        print(f"[ERROR Batch {batch_id}] {e}")


# ═══════════════════════════════════════════════════════════════
# OUTPUT B — Windowed counts → velocity detection
#            Writes to: trending_windows, velocity_alerts
# ═══════════════════════════════════════════════════════════════
def save_window(batch_df, batch_id):
    try:
        rows = batch_df.orderBy(desc("count")).collect()
        if not rows: return

        client, db = get_db()
        now = datetime.datetime.utcnow()

        window_docs = []
        for row in rows:
            win_start = str(row["window"]["start"])
            win_end   = str(row["window"]["end"])
            wc        = int(row["count"])

            db.trending_windows.update_one(
                {"word": row["word"], "window_start": win_start},
                {"$set": {"window_end": win_end, "window_count": wc, "recorded_at": now}},
                upsert=True
            )
            window_docs.append({"word": row["word"], "count": wc})

        # ── velocity: compare current window vs previous window ──
        velocity_list = []
        for doc in window_docs:
            word  = doc["word"]
            curr  = doc["count"]
            prev_doc = db.trending_windows.find_one(
                {"word": word},
                sort=[("window_start", -1)],
                skip=1
            )
            prev = prev_doc["window_count"] if prev_doc else 0
            if prev > 0:
                pct_change = round(((curr - prev) / prev) * 100, 1)
            else:
                pct_change = 100.0 if curr > 0 else 0.0

            velocity_list.append({
                "word": word, "current": curr,
                "previous": prev, "pct_change": pct_change
            })

        # sort by fastest rising
        velocity_list.sort(key=lambda x: x["pct_change"], reverse=True)

        db.velocity_alerts.replace_one(
            {"_id": "latest"},
            {"_id": "latest", "data": velocity_list[:10], "updated_at": now},
            upsert=True
        )

        client.close()
        if velocity_list:
            top = velocity_list[0]
            print(f"[Velocity Batch {batch_id}] Fastest rising: {top['word']} +{top['pct_change']}%")

    except Exception as e:
        print(f"[ERROR Window Batch {batch_id}] {e}")


# ═══════════════════════════════════════════════════════════════
# OUTPUT C — Hourly counts → peak hour detection
#            Writes to: hourly_stats, peak_hours
# ═══════════════════════════════════════════════════════════════
def save_hourly(batch_df, batch_id):
    try:
        rows = batch_df.collect()
        if not rows: return

        client, db = get_db()
        now = datetime.datetime.utcnow()

        for row in rows:
            db.hourly_stats.update_one(
                {"hour": int(row["hour_bucket"]), "word": row["word"]},
                {
                    "$inc": {"count": int(row["count"])},
                    "$set": {"last_updated": now}
                },
                upsert=True
            )

        # ── compute peak hours summary (which hour has most activity) ──
        pipeline = [
            {"$group": {"_id": "$hour", "total": {"$sum": "$count"}}},
            {"$sort": {"total": -1}}
        ]
        hour_totals = list(db.hourly_stats.aggregate(pipeline))

        if hour_totals:
            peak_hour = hour_totals[0]["_id"]
            peak_count = hour_totals[0]["total"]
            # build 24-hour array for chart
            hour_chart = [0] * 24
            for h in hour_totals:
                hour_chart[h["_id"]] = h["total"]

            db.peak_hours.replace_one(
                {"_id": "summary"},
                {
                    "_id": "summary",
                    "peak_hour": peak_hour,
                    "peak_count": peak_count,
                    "hour_chart": hour_chart,
                    "updated_at": now
                },
                upsert=True
            )
            print(f"[Hourly Batch {batch_id}] Peak hour: {peak_hour}:00 ({peak_count} mentions)")

        client.close()

    except Exception as e:
        print(f"[ERROR Hourly Batch {batch_id}] {e}")


# ═══════════════════════════════════════════════════════════════
# OUTPUT D — Co-occurrence pairs
#            Writes to: cooccurrence
# ═══════════════════════════════════════════════════════════════
def save_cooccurrence(batch_df, batch_id):
    try:
        rows = batch_df.collect()
        if not rows: return

        client, db = get_db()
        now = datetime.datetime.utcnow()
        pair_counts = {}

        for row in rows:
            tags = list(set(row["tags"]))  # deduplicate per tweet
            # generate all pairs
            for i in range(len(tags)):
                for j in range(i + 1, len(tags)):
                    a, b = sorted([tags[i], tags[j]])
                    key  = f"{a}||{b}"
                    pair_counts[key] = pair_counts.get(key, 0) + 1

        for pair_key, cnt in pair_counts.items():
            tag_a, tag_b = pair_key.split("||")
            db.cooccurrence.update_one(
                {"tag_a": tag_a, "tag_b": tag_b},
                {
                    "$inc": {"count": cnt},
                    "$set": {"last_seen": now}
                },
                upsert=True
            )

        # ── keep top-10 pairs summary for the API ──
        top_pairs = list(
            db.cooccurrence.find({}, {"_id": 0, "tag_a": 1, "tag_b": 1, "count": 1})
            .sort("count", -1).limit(10)
        )
        db.cooccurrence_summary.replace_one(
            {"_id": "top10"},
            {"_id": "top10", "pairs": top_pairs, "updated_at": now},
            upsert=True
        )

        client.close()
        if pair_counts:
            top_key = max(pair_counts, key=pair_counts.get)
            print(f"[Co-occur Batch {batch_id}] Top pair: {top_key} ({pair_counts[top_key]}x)")

    except Exception as e:
        print(f"[ERROR Co-occur Batch {batch_id}] {e}")


# ═══════════════════════════════════════════════════════════════
# STREAM QUERIES — wire everything up
# ═══════════════════════════════════════════════════════════════
query1 = global_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_analytics) \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = window_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_window) \
    .trigger(processingTime="30 seconds") \
    .start()

query3 = hourly_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_hourly) \
    .trigger(processingTime="60 seconds") \
    .start()

query4 = cooccur_df.writeStream \
    .outputMode("append") \
    .foreachBatch(save_cooccurrence) \
    .trigger(processingTime="15 seconds") \
    .start()

print("=" * 55)
print("  Spark Analytics — 4 query streams active")
print("  Q1: Global counts      → trends, analytics_summary")
print("  Q2: Windowed velocity  → trending_windows, velocity_alerts")
print("  Q3: Hourly peak        → hourly_stats, peak_hours")
print("  Q4: Co-occurrence      → cooccurrence, cooccurrence_summary")
print("=" * 55)

spark.streams.awaitAnyTermination()

