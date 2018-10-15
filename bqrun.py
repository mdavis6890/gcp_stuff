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
parser.add_argument("-c", "--cache", action='store_true',
                    help="Allow cache")

args = parser.parse_args()
parser.parse_args()
if args.debug:
    logging.basicConfig(level=log_choices[args.debug])


query_file = args.f
cache = args.c
client = bigquery.Client()

query_list = []
with query_file:
    query_text = query_file.read()
    # Does a split as expected, but only keeps non-empty strings.
    query_list = [s for s in query_text.split(';') if len(s) > 0 ]

    total_runtime = 0.0
    total_bytes_billed = 0.0
    total_bytes_processed = 0.0
    for query in query_list:
        logging.debug("Running Query: {}".format(query))
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.use_query_cache = cache  # Don't cache for benchmarks
 
            job = client.query(query, job_config=job_config)
            job.result()  # waits for job to finish - serial execution
 
            td_sec = (job.ended - job.started).total_seconds()
            total_runtime = total_runtime + td_sec
            total_bytes_billed = total_bytes_billed + job.total_bytes_billed
            total_bytes_processed = total_bytes_processed + job.total_bytes_processed
 
            logging.info("Query completed in {} seconds. Cache: {}".format(td_sec, job.use_query_cache))
            logging.info("Bytes processed for this query: {}".format(job.total_bytes_processed))
            logging.info("Bytes billed for this query: {}".format(job.total_bytes_billed))
            logging.info("Total bytes billed so far: {}".format(total_bytes_billed))
            logging.info("Total bytes processed so far: {}.".format(total_bytes_processed))
            logging.info("Total runtime so far: {} seconds\n\n".format(total_runtime))
        except Exception as e:
            logging.error("Query Failed: {}".format(query))
            logging.error(e)

print("Allow Cache: {}".format(cache))
print("Total bytes billed for this set of queries: {}".format(total_bytes_billed))
print("Total bytes processed for this set of queries: {}.".format(total_bytes_processed))
print("Total runtime for this set of queries: {} seconds\n\n".format(total_runtime))
print("Note that only successful queries are included in these totals.")

