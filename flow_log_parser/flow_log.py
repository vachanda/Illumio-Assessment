from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.types import Integer, String


Base = declarative_base()


class FlowLog(Base):
    """
    Flow Log table
    """
    __tablename__ = "flowlog"

    id = Column(Integer, autoincrement=True, primary_key=True)
    port = Column(Integer, nullable=False)
    protocol = Column(String(255), nullable=False)
    tag = Column(String(255), nullable=True, index=True, default="Untagged")
    count = Column(Integer, nullable=False, default=0)
    
    __table_args__ = (
        UniqueConstraint("port", "protocol", name="unique_fields"),
    )


def create_table(db_engine):
    Base.metadata.create_all(db_engine)
