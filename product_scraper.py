import csv
import logging
import sys
import urllib2
from Queue import Queue
from threading import Thread

from bs4 import BeautifulSoup

from product_backfill import report_count


THREADS = 10


#writer.writerow((asin, title, description, img_url, small_img_url))

class LookupThread(Thread):

    def __init__(self, inqueue, outqueue, group=None, target=None, name=None,
                 verbose=None,):
        Thread.__init__(self, group=group, target=target, name=name,
                        verbose=verbose)
        self.inqueue = inqueue
        self.outqueue = outqueue
        self.running = True
        self.processing = False

    def run(self):
        while self.running:
            logging.debug("Waiting for item")
            asin = self.inqueue.get()
            if not asin:
                break
            self.processing = True
            logging.info("Looking up %s", asin)
            lookup(asin, self.outqueue)
            self.processing = False


class WriterThread(Thread):
    def __init__(self, csv_writer, queue):
        Thread.__init__(self)
        self.queue = queue
        self.writer = csv_writer
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            line = self.queue.get()
            if line is None:
                break
            self.writer.writerow(line)


def lookup(asin, outqueue):
    url = "http://lon.gr/ata/%s" % asin
    logging.info("Looking up %s..." % url)
    response = urllib2.urlopen(url)
    xml = response.read()
    soup = BeautifulSoup(xml)
    try:
        doc = soup.document
        item = doc.item
        title = item.title.text.encode("UTF-8")
        description = item.description.text.encode("UTF-8")
        links = doc.links
        img_url = links.imgurl.text
        small_img_url = links.images.smallimage.text
        outqueue.put((asin, title, description, img_url, small_img_url))
        return True
    except AttributeError:
        return False


def main():
    logging.basicConfig(format='%(levelname)s:%(threadName)s:%(message)s',
                        level=logging.INFO)
    infilename="fastfoods.txt"
    outfilename="products.csv"
    if len(sys.argv) > 1:
        infilename = sys.argv[1]
    if len(sys.argv) > 2:
        outfilename = sys.argv[2]

    csv.register_dialect('cassandra', doublequote=False, escapechar="\\",
                         quoting=csv.QUOTE_NONE)

    to_lookup = Queue(1)
    to_write = Queue()
    stored = set()
    written = 0

    threads = [LookupThread(to_lookup, to_write, name="lookup%d" % x)
               for x in range(THREADS)]
    for thread in threads:
        thread.start()

    logging.info("Readening")

    with open(infilename, 'r') as infile:
        with open(outfilename, 'wb') as outfile:
            writer = csv.writer(outfile, dialect='cassandra')
            writer_thread = WriterThread(writer, to_write)
            writer_thread.start()
            for line in report_count(infile):
                line = line.strip()
                line = line.decode("ISO8859", "ignore")
                if line:
                    name, value = line.split(" ", 1)
                    name = name.strip(':')
                    column = name.split("/")
                    if len(column) > 1:
                        if column[1] == 'productId' and value not in stored:
                            to_lookup.put(value)
                            stored.add(value)
                            written += 1
            logging.info("Killing off lookup threads")
            for thread in threads:
                # Add in a kill message for each thread
                logging.debug("Sending empty message to %s", thread.name)
                to_lookup.put(None)
            # Make sure all the lookup threads are done
            for thread in threads:
                thread.join()

            # And for the writer
            logging.info("Killing off write queue")
            writer_thread.queue.put(None)
            writer_thread.join()
    logging.info("Done")

    print "Saw %d records, wrote %d" % (len(stored), written)


if __name__=='__main__':
    main()
