from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es = Elasticsearch("http://172.23.32.103:9200")
    count = 0
    deport = []
    year = "2020"
    month = ['.12', '.01', '.02', '.03', '.04', '.05', '.06', '.07', '.08', '.09', '.10', '.11', '.12']
    n = 0
    i = 0
    query = {'query':
                 {'term': {'request': 'jsonrpc'}}
             }
    while n < 13:
        result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=3800000)
        for log in result["hits"]["hits"]:
            deport.append(log["_source"]["destination_port"])
            deport = list(set(deport))
            # query['query']['bool']['must_not'].append({'term': {'destination_port': log[0]["_source"]["destination_port"]}})
        n = n + 1
        if n == 1:
            year = "2021"
        print(len(deport))
    print(len(deport))
