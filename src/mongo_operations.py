from sql_alchemy_operations import *
from utils.utils import *


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = company_id
        self.collection = sli_rev_mongo_db[str(company_id)]
        self.collection.create_index([("revisiondpid", pymongo.ASCENDING)])

    def insert_orm_data_into_mongo(self, chunk_size):
        query = get_data_using_orm(self.company_id)

        try:
            for each_chunk in get_data_in_chunks(query, chunk_size):
                self.collection.insert_many(each_chunk[:])
        except Exception as ex:
            print ex

    def insert_raw_data_into_mongo(self):
        data = get_data_using_raw_query(self.company_id)

        start = time.time()
        while True:
            data_chunk = data.fetchmany(1000)

            if not data_chunk:
                break

            result = [dict(row) for row in data_chunk]
            self.collection.insert_many(result)

        print PRINT_FORMAT.format(message="Mongo inseetion", time=time.time() - start)

    def get_rev_dp_data(self):
        query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression " \
                "FROM `{companyid}` WHERE revisiondpid IN (5244723122, 5052546311, 5052232448)"
        # query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression " \
        #         "FROM `{companyid}` WHERE revisiondpid IN (5052226683)"

        get_raw_query_per(self.company_id, query, "SQLAlchemy full data ")

        start = time.time()
        data = list(self.collection.find({
            "revisiondpid": {
                "$in": [
                    5244723122, 5052546311, 5052232448
                ]
            }
        }))

        # data = list(self.collection.find({
        #     "revisiondpid": {
        #         "$in": [
        #             5052226683
        #         ]
        #     }
        # }))

        print PRINT_FORMAT.format(message="Mongo rev data", time=time.time() - start)

        return data

    def get_full_data(self):
        query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression " \
                "FROM `{companyid}`"

        get_raw_query_per(self.company_id, query, "SQLAlchemy full data ")

        start = time.time()
        data = list(self.collection.find())
        print PRINT_FORMAT.format(message="Mongo full data", time=time.time() - start)

    def get_selected_col_data(self):
        query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression " \
                "FROM `{companyid}` WHERE periodstart='FY-2013'"

        get_raw_query_per(self.company_id, query, "SQLAlchemy full data ")

        required_cols = {'_id': 0, 'revisiondpid': 1, 'periodstart': 1, 'expression': 1, 'sourceid': 1}

        start = time.time()
        data = list(self.collection.find({}, required_cols))
        print PRINT_FORMAT.format(message="Mongo column filter data ", time=time.time() - start)

    def filter_data(self):
        query = "SELECT sliparameterid, periodstart, sourceid, revisiondpid, expression " \
                "FROM `{companyid}` WHERE periodstart='FY-2013'"

        get_raw_query_per(self.company_id, query, "SQLAlchemy column filter data ")

        start = time.time()
        data = list(self.collection.find({'periodstart': "FY-2013"}))
        print PRINT_FORMAT.format(message="Mongo filtered data", time=time.time() - start)

    def insert_data_using_trans(self):
        data = get_data_using_raw_query(self.company_id)

        with mongo_client.start_session() as s:
            s.start_transaction()

            while True:
                data_chunk = data.fetchmany(1000)

                if not data_chunk:
                    break

                result = [dict(row) for row in data_chunk]
                self.collection.insert_many(result, session=s)

            s.commit_transaction()

    def create_mongo_db_heirarchical_data(self):
        data = get_data_using_query(self.company_id)
        params = list()
        # self.collection = sli_rev_mongo_db[str(13060)]

        for each_data in data:
            revdpid = each_data.revisiondpid

            # if view_name != "SAR":
            #     exp = each_data.expression

            if each_data.expression is None:
                continue

            key, group_data = parse_exp(each_data.expression)

            params.append({'key': key, 'revdpid': revdpid, 'made_from': group_data})

            # params.append({"key": key, "revdpid": revdpid, "exp": exp, "child": group_data})

        # return params

        self.collection.insert_many(params)


if __name__ == '__main__':
    obj = MongoDBOperations(13059)

    # obj.insert_raw_data_into_mongo()
    # obj.get_full_data()
    # obj.filter_data()
    # obj.get_selected_col_data()
    # obj.get_rev_dp_data()

    # obj.get_data_using_trans()
    # obj.insert_data_using_trans()
    obj.create_mongo_db_heirarchical_data()
