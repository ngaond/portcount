from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es = Elasticsearch("http://172.23.32.103:9200")
    count = 0
    deport = []
    year = "2020"
    month = ['.12', '.01', '.02', '.03', '.04', '.05', '.06', '.07', '.08', '.09', '.10', '.11', '.12']
    up = 1
    down = 1000
    n = 0
    i = 0
    log = ['1']
    query = {'query':
                 {'term': {'request': 'jsonrpc'}}
             }
    while n < 13:
        while len(log) != 0:
            result = es.search(index="xpot_accesslog-" + year + month[n], body=query, size=1)
            log = result["hits"]["hits"]
            if len(log) != 0:
                deport.append(log[0]["_source"]["destination_port"])
                deport = list(set(deport))
                # query['query']['bool']['must_not'].append({'term': {'destination_port': log[0]["_source"]["destination_port"]}})
                count = count + 1
        n = n + 1
        if n == 1:
            year = "2021"
    print(deport)
