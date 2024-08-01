import csv
from collections import defaultdict

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from flow_log_parser.constants import *
from flow_log_parser.flow_log import FlowLog


class FlowLogParser:
    def __init__(self, lookup_file: str, logs_file: str, session: Session):
        self.lookup_csv_file = lookup_file
        self.session = session
        self.logs_file = logs_file

    def __get_csv_content(self) -> dict:
        with open(self.lookup_csv_file, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row

    def __get_tsv_content(self) -> tuple:
        with open(self.logs_file, "r") as file:
            reader = csv.reader(file, delimiter=' ')
            for row in reader:
                yield row[6].lower(), row[7].lower()

    def __flush_log_counts(self, count_dict: dict):
        batch = []
        for key, count in count_dict.items():
            data = {"port": key[0], "protocol": key[1], "count": count}
            batch.append(data)

        stmt = insert(FlowLog).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["port", "protocol"],
            set_=dict(count=(stmt.excluded.count + FlowLog.count))
        )

        self.session.execute(stmt)
        self.session.commit()

    def load_lookup_from_csv(self):
        batch = []
        for log in self.__get_csv_content():
            if len(batch) == INSERT_BATCH_SIZE:
                self.session.bulk_save_objects(batch)
                self.session.commit()
                batch = []
            else:
                data = FlowLog(port=log[CSV_DST_PORT], tag=log[CSV_TAG].lower(),
                               protocol=log[CSV_PROTOCOL].lower())
                batch.append(data)

        if batch:
            self.session.bulk_save_objects(batch)
            self.session.commit()

    def parse_flow_logs(self):
        port_protocol_dict = defaultdict(int)

        for pair in self.__get_tsv_content():
            port_protocol_dict[pair] += 1

            if len(port_protocol_dict) > 2:
                self.__flush_log_counts(port_protocol_dict)
                port_protocol_dict = defaultdict(int)

        if port_protocol_dict:
            self.__flush_log_counts(port_protocol_dict)
