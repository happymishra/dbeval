import collections
import re

from sql_alchemy_operations import *
from py2neo import Graph
graph = Graph(password='root')

cols = collections.namedtuple('DBEngine', ['engine', 'expr_col'])

DBEngine = {
    'SLI': cols(sli, ", expression"),
    'NMV': cols(nmv, ", expression"),
    'SAR': cols(sar, "")
}


# def get_data_using_raw_query(view_name, company_id, revisiondpid, parent_revdp=None):
#     engine_col = DBEngine.get(view_name)
#
#     engine = engine_col.engine
#     col = engine_col.expr_col
#
#     if revisiondpid:
#         query = "SELECT revisiondpid, value {expression} FROM `{companyid}` WHERE revisiondpid IN :id_list"
#     else:
#         query = "SELECT revisiondpid, value {expression} FROM `{companyid}`"
#
#
#     query = query.format(companyid=company_id, expression=col)
#
#     sli_rev_data = engine.execute(text(query), id_list=tuple(revisiondpid))
#
#     for each_data in sli_rev_data:
#         revdpid = each_data.revisiondpid
#
#         if view_name != "SAR":
#             exp = each_data.expression
#             create_node(view_name, revdpid, parent_revdp, exp)
#             key, group_data = parse_exp(exp)
#             get_data_using_raw_query(key, company_id, group_data, revdpid)
#
#         else:
#             create_node(view_name, revdpid, parent_revdp)


def get_all_data_using_raw_query(company_id, view_name="SLI", revisiondpid=None):
    engine_col = DBEngine.get(view_name)

    engine = engine_col.engine
    col = engine_col.expr_col

    if revisiondpid:
        query = "SELECT revisiondpid, value {expression} FROM `{companyid}` WHERE expression IS NOT NULL"
    else:
        query = "SELECT revisiondpid, value {expression} FROM `{companyid}` WHERE revisiondpid IN (5106327661)"
        # query = "SELECT revisiondpid, value {expression} FROM `{companyid}` WHERE expression IS NOT NULL LIMIT 700000"

    query = query.format(companyid=company_id, expression=col)

    sli_rev_data = engine.execute(text(query))

    params = list()

    for each_data in sli_rev_data:
        revdpid = each_data.revisiondpid

        if view_name != "SAR":
            exp = each_data.expression

        key, group_data = parse_exp(exp)

        params.append(create_node(view_name, key, revdpid, group_data, exp))

    # q = '''
    #     UNWIND {datas} AS data
    #     MERGE (n:SLI {key: data.key, revdpid: data.revdpid, child: data.child, exp: data.exp})
    #     WITH data, n
    #     MATCH (b:SLI)
    #     WHERE b.revdpid in n.child
    #     MERGE (n) -[:MADE_FROM]-> (b)
    # '''

    tx = graph.begin()

    q = '''
        UNWIND {datas} AS data
        CREATE (n:SLI {key: data.key, revdpid: data.revdpid, child: data.child, exp: data.exp})
    '''

    # q = '''
    #     FOREACH (data in $datas | MERGE (n:SLI {key: data.key, revdpid: data.revdpid, child: data.child, exp: data.exp}))
    # '''

    import time
    print "started"
    start = time.time()
    tx.run(q, parameters={"datas": params})

    print time.time() - start
    print "Creating relations"
    tx.run("""
        MATCH (n: SLI), (b:SLI)
        WHERE b.revdpid IN n.child
        CREATE (n) - [:MADE_FROM] -> (b)
    """)

    tx.commit()

    print  time.time() - start


def parse_exp(exp):
    group_data = list()
    parsed_exp = re.findall(FETCH_REV_DP_REGEX, exp)
    key = ''

    for each_exp in parsed_exp:
        key = each_exp[0]
        group_data.append(int(each_exp[1]))

    return key, group_data


def create_node(view_name, key, revdpid, group_data, exp=None):
    if exp:
        dict = {"key": key, "revdpid": revdpid, "exp": exp, "child":group_data}
    else:
        dict = {"key": key, "revdpid": revdpid, "child": group_data, "exp":''}

    # tx = graph.begin()
    #
    # q = "CREATE (n : " + view_name + " {params})"
    #
    # # tx.append(q, dict)
    # tx.run(q, parameters=dict)
    #
    # tx.commit()
    #
    # print "Node created: {0} {1}".format(revdpid, view_name)

    return dict

# def create_node(view_name, key, revdpid, group_data, exp=None):
#     # tx = graph.begin()
#     #
#     # q = "CREATE (n: {view_name})"
#     #
#     # tx.run("CREATE ()")
#
#     if exp:
#         dict = {'params': {'key': key, 'revdpid': revdpid, 'exp': exp, 'child':group_data}}
#     else:
#         dict = {'params': {'key': key, 'revdpid': revdpid, 'child': group_data}}
#
#     tx = graph.begin()
#
#     q = "CREATE (n : " + view_name + " {params})"
#
#     # tx.append(q, dict)
#     tx.run(q, parameters=dict)
#
#     tx.commit()
#
#     print "Node created: {0} {1}".format(revdpid, view_name)


def get_data_from_neo():
    import  time
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

    print time.time() - start
    print data



if __name__ == '__main__':
    get_data_from_neo()
    import time
    # start = time.time()
    # # get_data_using_raw_query("SLI", 13059, [5106327661])
    # print time.time() - start

    # get_all_data_using_raw_query(13059)

'''
MATCH (n: SLI {revdpid:3611496760})- [*]->(b)
WITH COLLECT (b) + n AS all UNWIND all as n
MATCH (n)-[:MADE_FROM]-> (b)
RETURN n.revdpid, {parent :n, all_child:collect(b.revdpid)}

'''
