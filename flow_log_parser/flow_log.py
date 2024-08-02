import logging


class FlowLog:
    """
    Flow Log DB model.
    """

    @staticmethod
    def create_table(conn):
        logging.info("Creating the FlowLog table.")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flowlog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port INTEGER NOT NULL,
                protocol VARCHAR(256) NOT NULL,
                tag VARCHAR(256) NOT NULL DEFAULT 'UNTAGGED',
                count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(port, protocol)
            )
        """)

        cursor.execute("""CREATE INDEX tag_idx ON flowlog (tag)""")

        conn.commit()
