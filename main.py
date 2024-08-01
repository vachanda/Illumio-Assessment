import argparse
import logging
import os.path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from flow_log_parser.flow_log import FlowLog, create_table
from flow_log_parser.flow_log_parser import FlowLogParser

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

DB_FILE = "flow_log.db"


def create_database() -> Session:
    engine = create_engine("sqlite:///{}".format(DB_FILE), echo=True)
    create_table(engine)
    logging.debug("Created the database.")

    return sessionmaker(bind=engine)()


def get_insights(session: Session, lookup_file: str, logs_file: str):
    flow_log_parser = FlowLogParser(session=session, logs_file=logs_file, lookup_file=lookup_file)

    flow_log_parser.load_lookup_from_csv()
    flow_log_parser.parse_flow_logs()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A Cli to interact with the flow log parser. The tool parses the "
                                                 "logs and generates the output files counts of different properties.")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-l", "--lookup-file", type=str, required=True, help="Lookup csv file path.")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input log file path.")

    parser_args, extras = parser.parse_known_args()

    if parser_args.debug:
        ch.setLevel(logging.DEBUG)

    if not os.path.exists(parser_args.input):
        logging.error("Please provide a valid input log file path.")
        sys.exit(1)

    if not os.path.exists(parser_args.lookup_file):
        logging.error("Please provide a valid lookup file path.")
        sys.exit(1)

    get_insights(create_database(), parser_args.lookup_file, parser_args.input)
