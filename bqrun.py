from google.cloud import bigquery
import logging
import argparse

parser = argparse.ArgumentParser()

log_choices = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
parser.add_argument("-d", "--debug", help="Set logging level.",
                            choices=log_choices)

parser.add_argument("-f", "--file", dest="f", metavar='filename',
                           help="File that contains the queries you want "
                                                   "to run.", type=argparse.FileType('r'),
                                                                           required=True)
args = parser.parse_args()
parser.parse_args()
if args.debug:
    logging.basicConfig(level=log_choices[args.debug])

query_file = args.f

query_list = []
with query_file:
    query_text = query_file.read()
    # Does a split as expected, but only keeps non-empty strings.
    query_list = [s for s in query_text.split(';') if len(s) > 0 ]


    client = bigquery.Client()
    total_runtime = 0.0
    for query in query_list:
        logging.debug("Running Query: {}".format(query))
        try:
            job = client.query(query)
            job.result() # waits for job to finish - serial execution
            td_sec = (job.ended - job.started).total_seconds()
            total_runtime = total_runtime + td_sec
            logging.info("Query completed in {} seconds.".format(td_sec))
            logging.info("Total runtime so far: {} seconds".format(total_runtime))
        except Exception as e:
            logging.error("Query Failed: {}".format(query))
            logging.error(e)

