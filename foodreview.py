#!/usr/bin/env python

import traceback

from flask import Flask, render_template
app = Flask(__name__)

@app.route("/")
def root():
    return "Hello World!"

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)

@app.route('/die/')
def die():
    # It's German for 'The Bart, The'
    raise AttributeError("You asked me to die and I did it")

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

