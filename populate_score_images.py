from cassandra.cluster import Cluster

from product_backfill import report_count

#MAX_PROCESS = 10
MAX_PROCESS = 999999


def main():
    cluster = Cluster(['54.68.233.239'])
    session = cluster.connect()
    session.set_keyspace('capstone')

    q = "SELECT tier, score, asin FROM products_by_score LIMIT %s"
    results = session.execute(q, (MAX_PROCESS,))

    search = session.prepare("""
    SELECT small_img_url FROM products WHERE asin=? LIMIT 1
    """)

    update = session.prepare("""
    UPDATE products_by_score SET img_url=?
    WHERE tier=? AND score=? AND asin=?
    """)

    for tier, score, asin in report_count(results, 50):
        result = session.execute(search, (asin,))
        small_img_url = result[0][0]
        session.execute(update, (small_img_url, tier, score, asin))

if __name__ == '__main__':
    main()
