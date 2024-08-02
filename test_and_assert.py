import csv
import logging
import os.path
import sys
import uuid
from collections import defaultdict
from random import choice

from main import parse_and_get_insights
from flow_log_parser.constants import *


PORT_LIMIT = 100
TAG_LIMIT = 75
LOG_COUNT = 100000
UNTAGGED_LOG_COUNT = 100

LOG_FILENAME = "generated-log-file.tsv"
LOOKUP_TABLE_FILE = "generated-lookup.csv"

PORTS = [str(x) for x in range(1, PORT_LIMIT + 1)]
PROTOCOLS = [uuid.uuid4().__str__() for _ in range(PORT_LIMIT)]
TAGS = [uuid.uuid4().__str__() for _ in range(TAG_LIMIT)]
TAGS.extend(["sv_P2", "SV_P2", "sv_P1", "SV_P1"])

VPC_FLOW_LOG_FORMAT = ("2\t123456789010\teni-1235b8ca123456789\t172.31.16.139\t172.31.16.21\t20641\t{}\t{}\t20\t4249"
                       "\t1418530010\t1418530070\tACCEPT\tOK\n")

TAG_COUNTS = defaultdict(int)
PORT_COUNTS = defaultdict(int)
PORT_TAG_MAP = {}


def generate_lookup_table():
    with open(LOOKUP_TABLE_FILE, "w+") as file:
        file.write("dstport,dstprotocol,tag\n")
        for port in PORTS:
            for protocol in PROTOCOLS:
                tag = choice(TAGS)
                PORT_TAG_MAP[(port, protocol)] = tag
                file.write("{},{},{}\n".format(port, protocol, tag))

    logging.info("Generated the lookup table with {} entries.".format(PORT_LIMIT * PORT_LIMIT))


def generate_log_files():
    with open(LOG_FILENAME, "w+") as file:
        for _ in range(LOG_COUNT):
            port, protocol = choice(PORTS), choice(PROTOCOLS)
            tag = PORT_TAG_MAP[(port, protocol)]

            PORT_COUNTS[(port, protocol)] += 1
            TAG_COUNTS[tag.lower()] += 1

            log = VPC_FLOW_LOG_FORMAT.format(port, protocol)
            file.write(log)

    logging.info("Generated the tagged log file with {} entries.".format(LOG_COUNT))

    with open(LOG_FILENAME, "a+") as file:
        for _ in range(UNTAGGED_LOG_COUNT):
            port, protocol = str(choice(PORTS) + str(PORT_LIMIT)), choice(PROTOCOLS)
            PORT_COUNTS[(port, protocol)] += 1

            log = VPC_FLOW_LOG_FORMAT.format(port, protocol)
            file.write(log)

    logging.info("Generated the untagged log file with {} entries.".format(UNTAGGED_LOG_COUNT))


def validate_tag_count():
    with (open(TAG_OUTPUT_FILE, "r") as file):
        reader = csv.DictReader(file, delimiter="\t")
        actual_untagged_count = 0

        for row in reader:
            if row[TAG_HEADER] == "UNTAGGED":
                actual_untagged_count = int(row[COUNT_HEADER])
                continue

            expected_count = TAG_COUNTS[row[TAG_HEADER].lower()]
            actual_count = int(row[COUNT_HEADER])
            assert expected_count == actual_count, "The tag count for {} does not match, expected={}, actual={}.".format(
                row[TAG_HEADER], expected_count, actual_count)

        assert actual_untagged_count == UNTAGGED_LOG_COUNT, "The untagged counts doest not match, expected={}, actual={}".format(
            UNTAGGED_LOG_COUNT, actual_untagged_count)

        logging.info("Successfully validated the tag counts.")


def validate_port_protocol_counts():
    actual_log_count = 0
    expected_total = LOG_COUNT + UNTAGGED_LOG_COUNT
    with (open(PORT_PROTOCOL_FILE, "r") as file):
        reader = csv.DictReader(file, delimiter="\t")

        for row in reader:
            expected_count = PORT_COUNTS[(row[PORT_HEADER], row[PROTOCOL_HEADER])]
            actual_count = int(row[COUNT_HEADER])
            actual_log_count += actual_count
            assert expected_count == actual_count, "The port-protocol count for {}-{} does not match, expected={}, actual={}.".format(
                row[PORT_HEADER], row[PROTOCOL_HEADER], expected_count, actual_count)

    assert actual_log_count == expected_total, "The total log count does not match, expected={}, actual={}".format(
        expected_total, actual_log_count)

    logging.info("Successfully validated the port-protocol counts.")


def test_log_parser():
    parse_and_get_insights(logs_file=LOG_FILENAME, lookup_file=LOOKUP_TABLE_FILE)
    validate_tag_count()
    validate_port_protocol_counts()


if __name__ == "__main__":
    if os.path.exists(LOOKUP_TABLE_FILE):
        os.remove(LOOKUP_TABLE_FILE)

    if os.path.exists(LOG_FILENAME):
        os.remove(LOG_FILENAME)

    generate_lookup_table()
    generate_log_files()
    test_log_parser()
