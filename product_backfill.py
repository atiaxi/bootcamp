#!/usr/bin/env python

# If you've already run product_loader, you can run this to re-do the work
# without having to make all the HTTP calls.  Should be faster (ideally)

from cassandra.cluster import Cluster

REPORT_FREQ = 500

def report_count(iterator, report_freq=REPORT_FREQ):
    for count, row in enumerate(iterator):
        yield row
        if count % report_freq == 0:
            print "%d: %s" % (count, row)


def main():
    cluster = Cluster(['54.68.233.239'])
    session = cluster.connect()
    session.set_keyspace('capstone')

    q = "SELECT asin, title, description, img_url FROM products limit 999999999"
    result = session.execute(q)

    update = session.prepare("""
    UPDATE reviews SET product_name = ?, product_description = ?,
      product_img_url = ? WHERE product_id = ?
    """)
    for asin, title, description, img_url in report_count(result):
        session.execute(update, (title, description, img_url, asin))

if __name__ == '__main__':
    main()
