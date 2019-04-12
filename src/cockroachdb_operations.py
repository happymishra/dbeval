from cockroachdb.sqlalchemy import run_transaction

from sql_alchemy_operations import *


class CockroachDBOperations:
    def __init__(self, company_id):
        self.company_id = company_id
        self.number = 0

    def get_rev_dp_data(self):
        query = 'SELECT * FROM "{companyid}" WHERE revisiondpid IN (5244723122, 5052546311, 5052232448)'
        # query = 'SELECT * FROM "{companyid}" WHERE revisiondpid IN (5052226683)'
        query = query.format(companyid=self.company_id)

        start = time.time()
        sli_rev_data = list(sli_rev_cock_engine.execute(text(query)))
        print PRINT_FORMAT.format(message="Cockroach rev data", time=time.time() - start)

        return sli_rev_data

    def get_filtered_data(self):
        query = 'SELECT * FROM "{companyid}" WHERE  periodstart =' + "'FY-2013'"
        # query = 'SELECT * FROM "{companyid}" WHERE revisiondpid IN (5052226683)'
        query = query.format(companyid=self.company_id)
        start = time.time()
        sli_rev_data = list(sli_rev_cock_engine.execute(text(query)))
        print PRINT_FORMAT.format(message="Cockroach filtered data", time=time.time() - start)

        return sli_rev_data

    def get_data(self):
        query = 'SELECT * FROM "{companyid}"'
        query = query.format(companyid=self.company_id)

        start = time.time()
        sli_rev_data = list(sli_rev_cock_engine.execute(text(query)))
        print PRINT_FORMAT.format(message="Cockroach full data", time=time.time() - start)

        return sli_rev_data

    def insert_full_data_into_cd(self, conn, data_to_insert):
        model = get_sli_revision_model(self.company_id, sli_rev_cock_session)

        start = time.time()

        while True:
            data_chunk = data_to_insert.fetchmany(100000)

            if not data_chunk:
                break

            # with sli_rev_cock_engine.connect() as conn:
            conn.execute(model.__table__.insert(), *data_chunk)

            # sli_rev_cock_engine.execute(model.__table__.insert(), *data_chunk)

        print PRINT_FORMAT.format(message="Insert full data", time=time.time() - start)

    def insert_data_into_cd(self, conn, data_chunk):
        self.number += 1
        model = get_sli_revision_model(self.company_id, sli_rev_cock_session)
        # data_to_insert = get_data_using_raw_query(self.company_id)

        # start = time.time()
        #
        # while True:
        #     data_chunk = data_to_insert.fetchmany(1000)
        #
        #     if not data_chunk:
        #         break

        # with sli_rev_cock_engine.connect() as conn:
        conn.execute(model.__table__.insert(), *data_chunk)

        # sli_rev_cock_engine.execute(model.__table__.insert(), *data_to_insert)
        # insert_bulk_data_result(sli_rev_cock_engine, model, data_to_insert)
        print "Inserted " + str(self.number)
        # print PRINT_FORMAT.format(message="Insert full data", time=time.time() - start)

    def call_run_transaction(self):
        data_to_insert = get_data_using_raw_query(self.company_id)

        print "Hello"

        start = time.time()
        while True:
            data_chunk = data_to_insert.fetchmany(100000)

            if not data_chunk:
                break

            print sli_rev_mysql_engine.pool.status()

            run_transaction(sli_rev_cock_engine, lambda s: self.insert_data_into_cd(s, data_chunk))

        print PRINT_FORMAT.format(message="Insert cockroach data", time=time.time() - start)

    def call_full_data_run_transaction(self):
        data_to_insert = get_data_using_raw_query(self.company_id)

        run_transaction(sli_rev_cock_engine, lambda s: self.insert_full_data_into_cd(s, data_to_insert))


if __name__ == '__main__':
    obj = CockroachDBOperations(13059)

    # obj.insert_data_into_cd()
    obj.call_run_transaction()

    # data_to_insert = get_data_using_raw_query(13059)
    #
    # run_transaction(sli_rev_cock_engine,
    #                 lambda s: obj.insert_data_into_cd(s))
    # data = obj.get_data()
    # obj.get_rev_dp_data()

    # obj.get_filtered_data()
    # obj.call_full_data_run_transaction()
