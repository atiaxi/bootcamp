#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import urllib2
from datetime import datetime

from bs4 import BeautifulSoup
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
"""
<xml>
<Document>
<Item>
<ParentAsin/>
<Title>Dogswell Vitality Canned Dog Food Case Duck</Title>
<Price>$26.99</Price>
<Description>
Dogswell Vitality Canned Dog Food Case Made from premium, natural ingredients, Dogswell products are available in a variety of formulas which are healthy for your dog. Vitality contains flaxseed and vitamins to help maintain eyes, skin and coat. Dogswell meals are natural grain free food for dogs and is made with cage-free chicken, duck, and New Zealand lamb. Dogswell also supports better nutrient absorption due to the inclusion of chelated minterals and natural sources of glucosamine and chondroitin. Features · Natural grain free food for your dogs · Helps maintain eyes, skin and coat · Supports better nutrient absorption Item Specifications: Size 12/12.5 Oz Cans per Case Style Chicken/Sweet Potato, Duck/Sweet Potato, Lamb/Sweet Potato Ingredients Chicken, Chicken Broth, Water Sufficient for Processing, Chicken Liver, Dried Egg Product, Salmon (Source of Omega 3), Peas, Potato Starch, Sweet Potatoes, Carrots, Red Peppers, Guar Gum, Natural Flavor, Sodium Phosphate, Cranberries, Blueberries, Spinach, Zucchini, Tricalcium Phosphate, Canola Oil (Source of Omega 3), Garlic Powder, Flaxseed Oil (Source of Omega 3), Squash, Potassium Chloride, Taurine, Iron Amino Acid Chelate, Vitamin E Supplement, Zinc Amino Acid Chelate, Choline Chloride, Cobalt Amino Acid Chelate, Copper Amino Acid Chelate, Manganese Amino Acid Chelate, Riboflavin Supplement, Sodium Selenite, Thiamine Mononitrate, Vitamin A Supplement, Vitamin B-12 Supplement, Potassium Iodide, Biotin, Vitamin D-3 Supplement. Guaranteed Analysis Crude Protein (min)8.00% Crude Fat (min)4.00% Crude Fiber (max)1.00% Moisture (max)82.00% Linoleic Acid (Omega 6)* (min) 0.70% Linolenic Acid (Omega 3)* (min) 0.10%w
</Description>
</Item>
<Links>
<Url>
http://www.amazon.de/gp/product/B001E4KFG0?tag=longramzndata-21
</Url>
<ImgUrl>
http://ecx.images-amazon.com/images/I/312Dz3ByzpL.jpg
</ImgUrl>
<Images>
<SmallImage>
http://ecx.images-amazon.com/images/I/312Dz3ByzpL._SL75_.jpg
</SmallImage>
<MediumImage>
http://ecx.images-amazon.com/images/I/312Dz3ByzpL._SL160_.jpg
</MediumImage>
<LargeImage>
http://ecx.images-amazon.com/images/I/312Dz3ByzpL.jpg
</LargeImage>
</Images>
</Links>
</Document>
</xml>
"""


PRODUCTS = """
CREATE TABLE IF NOT EXISTS products(
  asin varchar PRIMARY KEY,
  title varchar,
  description varchar,
  img_url varchar
)
"""


def lookup(asin, insert_statement, update_statement, session):
    url = "http://lon.gr/ata/%s" % asin
    print "Looking up %s..." % url
    response = urllib2.urlopen(url)
    xml = response.read()
    soup = BeautifulSoup(xml)
    try:
        doc = soup.document
        item = doc.item
        title = item.title.text
        description = item.description.text
        links = doc.links
        img_url = links.imgurl.text
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        batch.add(insert_statement, (asin, title, description, img_url))
        batch.add(update_statement, (title, description, img_url, asin))
        session.execute(batch)
        print "   Data inserted"
        return True
    except AttributeError:
        print "  Bad XML; skipped"


def main():
    cluster = Cluster(['54.68.233.239'])
    session = cluster.connect()

    keyspace = """
    CREATE KEYSPACE IF NOT EXISTS capstone WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'Analytics': '1',
      'Solr': '1',
      'Cassandra': '2'
    };
    """
    session.execute(keyspace)
    session.set_keyspace('capstone')

    # Create ye tables
    session.execute(PRODUCTS)

    if len(sys.argv) <= 1:
        filename = "fastfoods.txt"
    else:
        filename = sys.argv[1]

    ps = session.prepare("""
    INSERT INTO products(asin, title, description, img_url)
      VALUES(?, ?, ?, ?)

    """)

    update = session.prepare("""
    UPDATE reviews SET product_name = ?, product_description = ?,
      product_img_url = ? WHERE product_id = ?
    """)


    REPORT_FREQ = 500
    stored = set()
    written = 0

    with open(filename, "r") as infile:
        for count, line in enumerate(infile):
            line = line.strip()
            line = line.decode("UTF-8", "ignore")
            if line:
                name, value = line.split(" ", 1)
                name = name.strip(':')
                column = name.split("/")
                if len(column) > 1:
                    if column[1] == 'productId' and value not in stored:
                        if lookup(value, ps, update, session):
                            written += 1
                        stored.add(value)
            if count % REPORT_FREQ == 0:
                print "%d: %s" % (count, line)
    print "Saw %d records, wrote %d" % (len(stored), written)

if __name__ == '__main__':
    main()
