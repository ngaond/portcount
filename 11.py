from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es = Elasticsearch("http://172.23.32.103:9200")
    count = 0
    deport = []
    year = "2021"
    month = ['.12', '.01', '.02', '.03', '.04', '.05', '.06', '.07', '.08', '.09', '.10', '.11', '.12']
    n = 7
    i = 0
    m = 0
    log1 = 1
    query2 = {'query': {'bool': {'must': {'term': {'request': 'jsonrpc'}},
                                 'must_not': {'term': {'source_ip': '0'}}}}}
    while log1 != 0:
        ip = 0
        result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=1)
        ip = result["hits"]["hits"]["_source"]["source_ip"]
        query2['query']['bool']['must_not'].append({'term': {'source_ip': ip}})
        log1 = len(result["hits"]["hits"])
        if log1 != 0:
            query = {'query': {
                'bool': {'must': [{'term': {'request': 'jsonrpc'}}, {'term': {'source_ip': ip}}]}},
                     'sort': {"@timestamp": {"order": "asc"}}}
            result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=100000)
            for log in result["hits"]["hits"]:
                deport.append(log["_source"]["destination_port"])
                deport = list(set(deport))
                time = log["_source"]['@timestamp']
            while len(result["hits"]["hits"]) != 0:
                query = {'query': {'bool': {'must': [{'term': {'request': 'jsonrpc'}}, {'term': {'source_ip': ip}}]}},
                         'search_after': [time],
                         'sort': {"@timestamp": {"order": "asc"}},
                         }
                result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=100000)
                for log in result["hits"]["hits"]:
                    deport.append(log["_source"]["destination_port"])
                    deport = list(set(deport))
                    time = log["_source"]['@timestamp']
            print(len(deport))
    print(len(deport))
