import csv
import logging
from collections import defaultdict
from sqlite3 import Connection

from flow_log_parser.constants import *


class FlowLogParser:
    """
    Class responsible for loading and parsing VPC flow logs.
    The structure of the flow logs - https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html

    Sample -
    2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK

    The class only parses out the 6 and 7 Column containing the dstport and protocol.
    """
    def __init__(self, lookup_file: str, logs_file: str, conn: Connection):
        self.lookup_csv_file = lookup_file
        self.session = conn
        self.logs_file = logs_file

    def __get_csv_content(self) -> tuple:
        """
        Generator to load the lookup table csv.
        :return: tuple, (dstport, protocol, tag)
        """
        logging.debug("Getting the lookup table content from the file: {}".format(self.lookup_csv_file))
        with open(self.lookup_csv_file, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row

    def __get_tsv_content(self) -> tuple:
        """
        Generator to load the log file tsv.
        :return: tuple(str, str) -> (dstport, protocol)
        """
        logging.debug("Getting the log file from the file: {}".format(self.logs_file))
        with open(self.logs_file, "r") as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                yield row[DST_PORT_COL].lower(), row[DST_PROTOCOL_COL].lower()

    def __insert_with_tag(self, records: list[tuple]) -> None:
        """
        Inserts the tag lookup table entries into the DB.
        :param records: [(dstport, dstprotocol, tag)]
        """
        cursor = self.session.cursor()
        try:
            cursor.executemany(
                '''INSERT INTO flowlog (port, protocol, tag) VALUES (?, ?, ?)''', records
            )

            self.session.commit()
            cursor.close()
        except Exception as exp:
            logging.error("Failed to insert the lookup table entries into the table, exp: {}".format(exp))
            self.session.rollback()

    def __upsert_with_count(self, dict_records: dict) -> None:
        """
        Inserts the parsed log counts into the DB.
        :param dict_records: {(dstport, dstprotocol): count}
        """
        cursor = self.session.cursor()
        records = [(k[0], k[1], count,) for k, count in dict_records.items()]
        logging.debug("Inserting the log count for {} tags in the DB".format(len(dict_records)))
        try:
            cursor.executemany(
                '''
                INSERT INTO flowlog (port, protocol, count) VALUES (?, ?, ?)
                ON CONFLICT(port, protocol) DO UPDATE
                SET count = count + excluded.count
                ''', records
            )

            self.session.commit()
            cursor.close()
            logging.debug("Successfully updated the log counts in the table.")
        except Exception as exp:
            logging.error("Failed to insert the parsed log counts into the table, exp: {}".format(exp))
            self.session.rollback()

    def __get_tag_counts(self, row_offset) -> list:
        """
        Gets the aggregated tag counts from DB.
        :return: [(tag, count)]
        """
        cursor = self.session.cursor()
        logging.debug("Getting the aggregated counts of the tags from DB.")

        try:
            cursor.execute(
                '''

                SELECT tag, SUM(count) FROM flowlog WHERE count > 0 GROUP BY lower(tag) ORDER BY tag LIMIT ? OFFSET ?
                ''', (SQL_SELECT_LIMIT, row_offset)
            )
            results = cursor.fetchall()
            cursor.close()
            logging.debug("Found {} tag counts in the DB.".format(len(results)))

            return results
        except Exception as exp:
            logging.error("Failed to fetch the tag counts from the database, exp:{}".format(exp))
            return []

    def __get_port_protocol_counts(self, row_offset) -> list:
        """
        Gets the port, protocol counts from DB.
        :return: [(port, protocol, count)]
        """
        cursor = self.session.cursor()
        logging.debug("Getting the port-protocol counts from the DB.")

        try:
            cursor.execute(
                '''
                SELECT port, protocol, count FROM flowlog WHERE count > 0 ORDER BY port ASC LIMIT ? OFFSET ?
                ''', (SQL_SELECT_LIMIT, row_offset)
            )

            records = cursor.fetchall()
            cursor.close()
            logging.debug("Found {} port-protocol counts in the DB".format(len(records)))

            return records
        except Exception as exp:
            logging.error("Failed to fetch the port-protocol counts from the database, exp:{}".format(exp))
            return []


    def load_lookup_from_csv(self) -> None:
        """
        Loads the lookup table into the DB.
        """
        batch = []
        logging.info("Loading the lookup table into the db.")
        count = 0

        for row in self.__get_csv_content():
            if len(batch) == INSERT_BATCH_SIZE:
                self.__insert_with_tag(batch)
                batch = [tuple(row.values())]
                count += 1
                logging.debug("Inserted a lookup table batch of {} into the table".format(INSERT_BATCH_SIZE))
            else:
                batch.append(tuple(row.values()))
                count += 1

        if batch:
            self.__insert_with_tag(batch)
        logging.info("Successfully ingested {} number of entries from lookup table.".format(count))

    def parse_flow_logs(self) -> None:
        """
        Loads the parsed log counts into the DB.
        :return:
        """
        port_protocol_dict = defaultdict(int)
        logging.debug("Parsing the logs and inserting to the DB.")
        count = 0

        for pair in self.__get_tsv_content():
            count += 1
            if len(port_protocol_dict) == INSERT_BATCH_SIZE:
                self.__upsert_with_count(port_protocol_dict)
                port_protocol_dict = defaultdict(int)
                port_protocol_dict[pair] += 1
            else:
                port_protocol_dict[pair] += 1

        if port_protocol_dict:
            self.__upsert_with_count(port_protocol_dict)

        logging.info("Successfully parsed {} log messages from the input file.".format(count))

    def get_tag_counts(self) -> None:
        """
        Generates the tag counts file from the DB.
        """
        row_offset = 0

        records = self.__get_tag_counts(row_offset)
        if not records:
            logging.warning("No tag records found in DB to generate the output.")
            return

        with open(TAG_OUTPUT_FILE, "a+") as file:
            file.write("{}\t{}\n".format(TAG_HEADER, COUNT_HEADER))

            while records:
                tsv_data = ["{}\t{}\n".format(val[0], val[1]) for val in records]
                tsv_data = "".join(tsv_data)
                file.write(tsv_data)
                file.flush()

                row_offset += SQL_SELECT_LIMIT
                records = self.__get_tag_counts(row_offset)

    def get_port_protocol_counts(self) -> None:
        """
        Generates the port-protocol counts file from the DB.
        """
        row_offset = 0
        records = self.__get_port_protocol_counts(row_offset)

        if not records:
            logging.warning("No records for port,protocol found in DB to generate output.")
            return

        with open(PORT_PROTOCOL_FILE, "a+") as file:
            file.write("{}\t{}\t{}\n".format(PORT_HEADER, PROTOCOL_HEADER, COUNT_HEADER))

            while records:
                tsv_data = ["{}\t{}\t{}\n".format(val[0], val[1], val[2]) for val in records]
                tsv_data = "".join(tsv_data)
                file.write(tsv_data)
                file.flush()

                row_offset += SQL_SELECT_LIMIT
                records = self.__get_port_protocol_counts(row_offset)
