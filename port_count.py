from elasticsearch import Elasticsearch

# count number of destination_port with jsonrpc
# from 2020/12~2021/12--
#    | 1 month--
#    |  | 1 ip--
#    |  |    count number of destination_port

if __name__ == '__main__':
    es = Elasticsearch("http://172.23.32.103:9200")
    deport = []      # destination_port
    year = ["2020", "2021"]
    month = ['.12', '.01', '.02', '.03', '.04', '.05', '.06', '.07', '.08', '.09', '.10', '.11',
             '.12']  # 2020/12-2021/12
    n = 7            # month
    i = 1            # year    n=7 i=1 ---2021/7

    #   n=0
    #   i=0
    #   while  n < 13:                                             # month loop
    # month ip loop
    #      n = n + 1
    #      if n == 1:
    #         i = i + 1
    log1 = 1
    query2 = {'query': {'bool': {'must': {'term': {'request': 'jsonrpc'}},
                                 'must_not': [{'term': {'source_ip': '0'}}]}}}
    while log1 != 0:                                                                        # ip loop
        ip = ' '
        result = es.search(index="xpot_accesslog-" + year + month[n], body=query2, size=1)  # get 1 jsonrpc ip
        log1 = len(result["hits"]["hits"])
        if log1 != 0:
            ip = result["hits"]["hits"][0]["_source"]["source_ip"]
            query2['query']['bool']['must_not'].append({'term': {'source_ip': ip}})         # destination_port count
            query = {'query': {
                'bool': {'must': [{'term': {'request': 'jsonrpc'}}, {'term': {'source_ip': ip}}]}},
                'sort': {"@timestamp": {"order": "asc"}}}
            result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=10000)
            for log in result["hits"]["hits"]:
                deport.append(log["_source"]["destination_port"])  # port count
                deport = list(set(deport))                         # remove duplication
                time = log["_source"]['@timestamp']                # search after api tiebreaker
            while len(result["hits"]["hits"]) != 0:                # search_after api 1 loop count 10000 requests
                query = {'query': {'bool': {'must': [{'term': {'request': 'jsonrpc'}}, {'term': {'source_ip': ip}}]}},
                         'search_after': [time],
                         'sort': {"@timestamp": {"order": "asc"}},
                         }
                result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=10000)
                for log in result["hits"]["hits"]:
                    deport.append(log["_source"]["destination_port"])
                    deport = list(set(deport))
                    time = log["_source"]['@timestamp']             # new tiebreaker
            print(len(deport))                            # number of 1 ip destination_port
    print(len(deport))                                    # number of 1 month destination_port
