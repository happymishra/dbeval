from sqlalchemy import Column, Integer, BigInteger, CHAR, DateTime, DECIMAL
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from constants import *
import pymongo


def get_mysql_engine(db="default"):
    db_params = dict(config.items(db))
    conn_string = MYSQL_DB_URL.format(db_params['user'], db_params['password'], db_params['host'],
                                      db_params['database'])

    return create_engine(conn_string, pool_pre_ping=True)


def get_local_mysql_engine():
    conn_string = MYSQL_DB_URL.format("root", "root", "localhost", "sli_revision")

    return create_engine(conn_string, pool_pre_ping=True)


def get_cockroach_engine():
    connect_args = {'sslmode': 'disable'}

    conn_string = COCKROACH_DB_URL.format("rupesh", "localhost", "26257", "sli_revision")
    return create_engine(conn_string, connect_args=connect_args)


def get_mongo_client(db=SLI_REVISION_DB):
    conn_string = MONGO_DB_URL.format("localhost", "27017")

    return pymongo.MongoClient(conn_string)


def get_session(engine):
    session_factory = sessionmaker(bind=engine)

    return scoped_session(session_factory)


sli_rev_mysql_engine = get_mysql_engine(SLI_REVISION_DB)
sli_rev_mysql_session = get_session(sli_rev_mysql_engine)

sli_rev_cock_engine = get_cockroach_engine()
sli_rev_cock_session = get_session(sli_rev_cock_engine)

mongo_client = get_mongo_client()
sli_rev_mongo_db = mongo_client[SLI_REVISION_DB]

local_sli_revision = get_local_mysql_engine()
local_sli_revision_session = get_session(local_sli_revision)


class _Base(object):
    class_registry = dict()

    @classmethod
    def set_metadata(cls, db_session, table_name):
        cls.db_session = db_session
        cls.__tablename__ = '{table_name}'.format(table_name=table_name)
        cls.query = db_session.query_property()

        return cls


def get_sli_revision_model(table_name, engine):
    base = declarative_base(cls=_Base.set_metadata(engine, table_name))

    class SLIRevision(base):
        revisiondpid = Column(BigInteger, primary_key=True)
        latestdpid = Column(BigInteger)
        datapointtype = Column(CHAR)
        effectivestart = Column(DateTime)
        effectiveend = Column(DateTime)
        expression = Column(CHAR)
        periodstart = Column(CHAR)
        sliparameterid = Column(Integer)
        value = Column(DECIMAL())
        sourceid = Column(Integer)
        isimplied = Column(Integer)
        annotationsettings = Column(CHAR)

    return SLIRevision


# =============================== Neo4j ==================================================
sli = get_mysql_engine(SLI_REVISION_DB)
nmv = get_mysql_engine(NMV)
sar = get_mysql_engine(SAR)


# from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom
#
#
# class SLIRevisionGraph(GraphObject):
#     revision_dpid = Property()
#     expression = Property()
#
#     sli = RelatedTo("SLIRevisionGraph", "MADE_FROM")
#     nmv = RelatedTo(NMVGraph, "MADE_FROM")
#
#
# class NMVGraph(GraphObject):
#     revision_dpid = Property()
#     expression = Property()
#
#     nmv = RelatedTo("NMVGraph", "MADE_FROM")
