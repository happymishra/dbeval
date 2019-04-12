import collections
import re

from py2neo import Graph

from sql_alchemy_operations import *

graph = Graph(password='root')

cols = collections.namedtuple('DBEngine', ['engine', 'expr_col'])


class Neo4j:
    def __init__(self, company_id):
        self.company_id = company_id

        self.DBEngine = {
            'SLI': cols(sli, ", expression"),
            'NMV': cols(nmv, ", expression"),
            'SAR': cols(sar, "")
        }

    def get_all_data_using_raw_query(self):
        params = list()
        view_name = "SLI"
        engine_col = self.DBEngine.get(view_name)
        engine = engine_col.engine
        col = engine_col.expr_col

        query = "SELECT revisiondpid, value {expression} FROM `{companyid}` WHERE expression IS NOT NULL"
        query = query.format(companyid=self.company_id, expression=col)

        sli_rev_data = engine.execute(text(query))

        for each_data in sli_rev_data:
            revdpid = each_data.revisiondpid

            if view_name != "SAR":
                exp = each_data.expression

            key, group_data = parse_exp(exp)

            params.append({"key": key, "revdpid": revdpid, "exp": exp, "child": group_data})

        return params

    def create_node_and_relation(self):
        data = self.get_all_data_using_raw_query()

        tx = graph.begin()

        create_node_query = '''
            UNWIND {datas} AS data
            CREATE (n:SLI {key: data.key, revdpid: data.revdpid, child: data.child, exp: data.exp})
        '''

        print "started"
        start = time.time()

        tx.run(create_node_query, parameters={"datas": data})

        print "Node creation time: " + str(time.time() - start)
        print "Creating relations"

        create_relations_query = """
            MATCH (n: SLI), (b:SLI)
            WHERE b.revdpid IN n.child
            CREATE (n) - [:MADE_FROM] -> (b)
        """

        tx.run(create_relations_query)
        tx.commit()

        print "Relations created and node creations time:" + str(time.time() - start)

    def get_data_from_neo(self):
        start = time.time()

        query = '''
            MATCH (n: SLI {revdpid:5106327661})- [*]->(b:SLI)
            WITH COLLECT (b) + n AS all UNWIND all as n
            MATCH (n)-[:MADE_FROM]-> (b)
            RETURN n.revdpid as r, {parent :n, all_child:collect(b.revdpid)} as d
        '''

        data = dict()
        result = graph.run(query)

        for each_record in result:
            data[each_record.get('r')] = {
                'all_childs': each_record.get('d').get('all_child'),
                'node': dict(each_record.get('d').get('parent'))
            }

        print "Time to get data from Neo4j: " + str(time.time() - start)

        print data


def parse_exp(exp):
    group_data = list()
    parsed_exp = re.findall(FETCH_REV_DP_REGEX, exp)
    key = ''

    for each_exp in parsed_exp:
        key = each_exp[0]
        group_data.append(int(each_exp[1]))

    return key, group_data


if __name__ == '__main__':
    obj = Neo4j(13059)

    obj.create_node_and_relation()
    obj.get_data_from_neo()

# This query is used to created node and relations at the same time
QUERY = '''
        UNWIND {datas} AS data
        MERGE (n:SLI {key: data.key, revdpid: data.revdpid, child: data.child, exp: data.exp})
        WITH data, n
        MATCH (b:SLI)
        WHERE b.revdpid in n.child
        MERGE (n) -[:MADE_FROM]-> (b)
'''
