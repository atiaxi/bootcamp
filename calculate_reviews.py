#!/usr/bin/env python

# Intended to be run any time after importer.py
# Calculates reviews for products and populates the reviews table.
from collections import defaultdict, Counter

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

from product_backfill import report_count

SCORES = """
CREATE TABLE IF NOT EXISTS products_by_score(
  tier FLOAT,
  score FLOAT,
  asin VARCHAR,
  title VARCHAR,
  img_url VARCHAR,
  PRIMARY KEY (tier, score, asin)
) WITH CLUSTERING ORDER BY (score DESC)
"""

MAX_REVIEWS = 999999999
#MAX_REVIEWS = 10


def calculate_tier(value, granularity=0.5):
    tier = int(value / float(granularity))
    key = tier * granularity
    return key


def main():
    cluster = Cluster(['54.68.233.239'])
    session = cluster.connect()
    session.set_keyspace('capstone')

    session.execute(SCORES)

    # First, calculate all the review scores
    scores = defaultdict(Counter)

    q = """
      SELECT score, product_id, product_name, product_img_url
        FROM reviews LIMIT %d
    """ % MAX_REVIEWS
    result = session.execute(q)
    for score, asin, title, img_url in report_count(result):
        scores[asin]["num_reviews"] += 1
        scores[asin]["num_stars"] += score or 0
        scores[asin]["title"] = title
        scores[asin]["img_url"] = img_url
    print "----------------------------------------"
    print "Writing average stats back"

    review = """
        UPDATE reviews SET num_reviews = ?, num_stars = ?, avg_reviews = ?
          WHERE product_id = ?
    """
    review_statement = session.prepare(review)
    score = """
        INSERT INTO products_by_score(tier, score, asin, title, img_url)
          VALUES(?, ?, ?, ?, ?)
    """
    score_statement = session.prepare(score)

    for asin in report_count(scores):
        num_reviews = scores[asin]["num_reviews"]
        num_stars = scores[asin]["num_stars"]
        title = scores[asin]['title']
        # If there's no information in here, don't put it on the frontpage
        if not title:
            continue
        img_url = scores[asin]['img_url']
        possible_stars = num_reviews * 5
        avg_reviews = (num_stars / float(possible_stars)) * 5
        tier = calculate_tier(avg_reviews)
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        batch.add(review_statement, (num_reviews, num_stars, avg_reviews, asin))
        batch.add(score_statement, (tier, avg_reviews, asin, title, img_url))
        session.execute(batch)

if __name__ == '__main__':
    main()
