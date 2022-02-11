from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es = Elasticsearch("http://172.23.32.103:9200")
    count = 0
    deport = []
    year = "2021"
    month = ['.12', '.01', '.02', '.03', '.04', '.05', '.06', '.07', '.08', '.09', '.10', '.11', '.12']
    n = 7
    i = 0
    query = {'query': {'bool':{'must':[{'term': {'request': 'jsonrpc'}},{'term': {'source_ip': '206.81.15.31'}}]}},
             'sort': {"@timestamp": {"order": "asc"}}}
    result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=100000)
    for log in result["hits"]["hits"]:
        deport.append(log["_source"]["destination_port"])
        deport = list(set(deport))
        time = log["_source"]['@timestamp']
        # ip = log["_source"]['source_ip']
    while len(result["hits"]["hits"]) != 0:
        query1 = {'query': {'bool':{'must':[{'term': {'request': 'jsonrpc'}},{'term': {'source_ip': '206.81.15.31'}}]}},
                 'search_after': [time],
                 'sort': {"@timestamp": {"order": "asc"}},
                 }
        result = es.search(index="xpot_accesslog-" + year + month[n], body=query1, size=100000)
        for log in result["hits"]["hits"]:
            deport.append(log["_source"]["destination_port"])
            deport = list(set(deport))
            time = log["_source"]['@timestamp']
            # ip = log["_source"]['source_ip']
    print(len(deport))
