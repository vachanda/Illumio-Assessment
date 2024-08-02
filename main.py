import argparse
import logging
import os.path
import sys
from sqlite3 import connect, Connection

from flow_log_parser.constants import TAG_OUTPUT_FILE, PORT_PROTOCOL_FILE
from flow_log_parser.flow_log import FlowLog
from flow_log_parser.flow_log_parser import FlowLogParser

root = logging.getLogger("FlowLogParser")
root.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(filename)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

root.addHandler(ch)

DB_FILE = "flow_log.db"


def cleanup_workdir():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    if os.path.exists(TAG_OUTPUT_FILE):
        os.remove(TAG_OUTPUT_FILE)

    if os.path.exists(PORT_PROTOCOL_FILE):
        os.remove(PORT_PROTOCOL_FILE)


def create_database() -> Connection:
    engine = connect(DB_FILE)
    FlowLog.create_table(engine)

    return engine


def parse_and_get_insights(lookup_file: str, logs_file: str):
    cleanup_workdir()
    session = create_database()
    flow_log_parser = FlowLogParser(conn=session, logs_file=logs_file, lookup_file=lookup_file)

    flow_log_parser.load_lookup_from_csv()
    flow_log_parser.parse_flow_logs()
    flow_log_parser.get_tag_counts()
    flow_log_parser.get_port_protocol_counts()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A Cli to interact with the flow log parser. The tool parses the "
                                                 "logs and generates the output files counts of different properties.")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-l", "--lookup-file", type=str, required=True, help="Lookup csv file path.")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input log file path.")

    parser_args, extras = parser.parse_known_args()

    if parser_args.debug:
        ch.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)

    if not os.path.exists(parser_args.input):
        logging.error("Please provide a valid input log file path.")
        sys.exit(1)

    if not os.path.exists(parser_args.lookup_file):
        logging.error("Please provide a valid lookup file path.")
        sys.exit(1)

    parse_and_get_insights(parser_args.lookup_file, parser_args.input)
