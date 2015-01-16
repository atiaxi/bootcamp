#!/usr/bin/env python

import traceback

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from flask import Flask, abort, g, redirect, render_template

from calculate_reviews import calculate_tier
from config import Config


app = Flask(__name__)


@app.route("/")
@app.route("/scores/<float:score>")
def redirect_to_scores(score=5.0):
    score = float(score)
    return redirect("/scores/%f/0" % score)


@app.route("/scores/<float:score>/<int:page>")
def products_by_score(score=5.0, page=0):
    tier = calculate_tier(float(score))
    page = int(page)

    # This is an especially stupid way of paginating, but doing it the
    # Right Way (TM) would require data model changes, and I've done
    # way too many of those already.
    limit = 10
    max = (page + 1) * limit


    q = """SELECT score, asin, title, img_url FROM products_by_score
              WHERE tier=%s LIMIT %s"""
    results = g.session.execute(q, (tier,max, ))
    results = list(results)[-limit:]
    print "Page is %d" % page
    return render_template('products_by_score.html',
                           tier=tier, products=results, page=page)


@app.route("/review/<asin>")
def reviews_by_products(asin):
    # Data about this product in particular
    q = """SELECT product_name, product_description,
        product_img_url, avg_reviews
        FROM reviews WHERE product_id=%s LIMIT 1"""
    result = g.session.execute(q, (asin,))
    if not result:
        abort(404)
    product_info = result[0]

    # And all the reviews
    q = """SELECT profile_name, helpfulness, score, time, summary, text
        FROM reviews WHERE product_id=%s"""
    results = g.session.execute(q, (asin,))


    return render_template('reviews_by_products.html',
                           info=product_info, results=results)


# Product search by title  - ATourkow
@app.route("/products/search/")
@app.route("/products/search/<search>")
def products_search_redirect(search=""):
    return redirect("/products/search/%s/0" % search)

@app.route("/products/search/<search>/<int:page>")
def products_search_by_title(search="", page=0):
    limit = 10
    start = page * limit
    search_replaced = search.replace (" ", "+")
    solr_query = '{"q":"title:%s*", "start":%s}'%(search_replaced, start)
    query = SimpleStatement("""
      SELECT * FROM products WHERE solr_query=%s LIMIT %s
    """, consistency_level=ConsistencyLevel.ONE)
    results = g.session.execute(query, (solr_query, limit))
    return render_template('products_search_by_title.html',
                           search=search, products=results, page=page)


@app.route('/die/')
def die():
    # It's German for 'The Bart, The'
    raise AttributeError("You asked me to die and I did it")

@app.before_request
def before_request():
    g.config = Config()
    policy = DCAwareRoundRobinPolicy()
    cluster = Cluster(g.config.servers_solr,
                      load_balancing_policy=policy)
    session = cluster.connect()
    session.set_keyspace(g.config.keyspace)
    g.session = session

@app.teardown_request
def teardown_request(exception):
    session = getattr(g, 'session', None)
    if session is not None:
        session.cluster.shutdown()
        session.shutdown()

@app.errorhandler(500)
def internal_error(exception):
    app.logger.exception(exception)
    return render_template('500.html', exc=traceback.format_exc()), 500

if __name__ == "__main__":
    import logging, sys
    logging.basicConfig(stream=sys.stderr)

    app.run(debug=True, use_debugger=True)

    import logging
    file_handler = logging.FileHandler('/var/log/capstone.log')
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

