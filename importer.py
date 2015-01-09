#!/usr/bin/env python
import sys
from datetime import datetime

from cassandra.cluster import Cluster

"""
product/productId: B001E4KFG0
review/userId: A3SGXH7AUHU8GW
review/profileName: delmartian
review/helpfulness: 1/1
review/score: 5.0
review/time: 1303862400
review/summary: Good Quality Dog Food
review/text: I have bought several of the Vitality canned dog food products and have found them all to be of good quality. The product looks more like a stew than a processed meat and it smells better. My Labrador is finicky and she appreciates this product better than  most.
"""

REVIEWS = """
CREATE TABLE IF NOT EXISTS reviews(
  product_id varchar,
  user_id varchar,
  profile_name varchar,
  helpfulness varchar,
  score FLOAT,
  time TIMESTAMP,
  summary VARCHAR,
  text VARCHAR,
  PRIMARY KEY (product_id, user_id)
)
"""


def main():
    cluster = Cluster(['54.68.233.239'])
    session = cluster.connect()

    keyspace = """
    CREATE KEYSPACE capstone WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'Analytics': '1',
      'Solr': '1',
      'Cassandra': '2'
    };
    """
    session.execute(keyspace)
    session.set_keyspace('capstone')

    # Create ye tables
    session.execute(REVIEWS)

    if len(sys.argv) <= 1:
        filename = "fastfoods.txt"
    else:
        filename = sys.argv[1]

    print sys.argv

    ps = session.prepare("""
    INSERT INTO reviews(product_id, user_id, profile_name, helpfulness,
      score, time, summary, text) VALUES(?, ?, ?, ?, ?, ?, ?, ?)

    """)

    REPORT_FREQ = 500

    with open(filename, "r") as infile:
        storage = {}
        for count, line in enumerate(infile):
            line = line.strip()
            line = line.decode("UTF-8", "ignore")
            if line:
                name, value = line.split(" ", 1)
                name = name.strip(':')
                column = name.split("/")
                if len(column) > 1:
                    column = column[1]
                    storage[column] = value
                else:
                    continue
            else:
                product_id = storage["productId"]
                user_id = storage["userId"]
                profile_name = storage["profileName"]
                helpfulness = storage["helpfulness"]
                score = float(storage["score"])
                ts = datetime.utcfromtimestamp(int(storage["time"]))
                summary = storage["summary"]
                text = storage["text"]

                args = [product_id, user_id, profile_name, helpfulness, score,
                        ts, summary, text]
                session.execute(ps, args)
                storage = {}
            if count % REPORT_FREQ == 0:
                print "%d: %s" % (count, line)

if __name__ == '__main__':
    main()
