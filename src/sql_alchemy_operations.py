import itertools
import time

from sqlalchemy.sql import *

from models import *


def get_raw_query_per(company_id, query, message):
    start = time.time()

    query = query.format(companyid=company_id)
    data = list(local_sli_revision.execute(text(query)))

    print PRINT_FORMAT.format(message=message, time=time.time() - start)


def get_data_using_raw_query(company_id):
    query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression FROM `{companyid}`"
    query = query.format(companyid=company_id)

    sli_rev_data = sli_rev_mysql_engine.execute(text(query))

    return sli_rev_data

# get_data_using_raw_query(13059)


def get_data_using_orm(company_id):
    model = get_sli_revision_model(company_id)
    sli_rev_data = model.query.with_entities(model.sliparameterid, model.periodstart, model.sourceid,
                                             model.revisiondpid,
                                             model.expression).limit(50)

    return sli_rev_data


def get_model_dict(model):
    return dict((column.name, getattr(model, column.name))
                for column in model.__table__.columns)


def get_data_in_chunks(iterable, size):
    it = iter([u._asdict() for u in iterable])
    item = list(itertools.islice(it, size))

    while item:
        yield item
        item = list(itertools.islice(it, size))


def get_data_using_query(company_id):
    query = '''
    SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression FROM slirevision.`13059`
    WHERE revisiondpid IN (5377554555, 5377557932, 5377557484)
    '''

    sli_rev_data = sli_rev_mysql_engine.execute(text(query))

    return sli_rev_data


def insert_bulk_data_result(engine, model, rows, chunk_size=1000):
    """
    Insert one table output into another table
    :param engine: DB engine which will execute the query
    :param model: Model of the table in which data will be inserted
    :param rows: Data which need to be inserted
    :param chunk_size: Chunk size by which data will be inserted
    """
    with engine.connect() as conn:
        with conn.begin() as trans:
            try:
                while True:
                    data_chunk = rows.fetchmany(chunk_size)

                    if not data_chunk:
                        break

                    conn.execute(model.__table__.insert(), *rows)

                trans.commit()
            except Exception as ex:
                trans.rollback()
                logging.error(str(ex))
                raise


# def insert_bulk_data_cockroachdb(engine, model, rows, chunk_size):
#     while True:
#         data_chunk = rows.fetchmany(chunk_size)
#
#         if not data_chunk:
#             break
#
#         conn.execute(model.__table__.insert(), *rows)


def insert_into_local_mysql():
    data = get_data_using_raw_query(13059)

    model = get_sli_revision_model(13059, local_sli_revision_session)

    start = time.time()
    insert_bulk_data_result(local_sli_revision, model, data)
    print PRINT_FORMAT.format(message="Local insert mysql", time=time.time() - start)

# insert_into_local_mysql()
