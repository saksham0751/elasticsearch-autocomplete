from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from es_autocomplete.indexer import ESIndexer


class AutoComplete(object):

    def __init__(self, es, doc_type, index, max_limit, config, *args, **kwargs):
        self.doc_type = doc_type
        self.es = es
        self.index = index
        self.max_limit = max_limit
        self.config = config
        self.args = args
        self.kwargs = kwargs

    def set_attr(self, op_type=None):
        self.es_indexer = ESIndexer(
            self.es,
            doc_type=self.doc_type,
            op_type=op_type,
            index=self.index,
            fields=self.config['FIELDS']['index_fields'],
            app=self.config['FIELDS']['app']
            )

    def set_attrs(self, op_type=None):
        self.es_indexer = ESIndexer(
            self.es,
            doc_type=self.doc_type,
            op_type=op_type,
            index=self.index,
            fields=self.config['FIELDS']['search_fields'],
            app=self.config['FIELDS']['app']
        )

    def set(self, docs, *args, **kwargs):
        self.set_attr('index')
        self.es_indexer.sync(docs, *args, **kwargs)

    def get(self, string=None, *args, **kwargs):
        self.set_attrs('index')
        fields = self.config['FIELDS']["search_fields"]
        fields = list(set(fields))
        count = self.max_limit
        filter_dict = kwargs.get('filter_dict', {})
        es = self.es
        search = Search(using=es, index=self.index, doc_type=self.doc_type)
        search = search[0:count]
        if string:
            q = Q("multi_match", query=string, fields=fields)
            search = search.query(q)
        for key, value in filter_dict.items():
            if isinstance(value, (str, int, bool)):
                search = search.filter("term", **{key: str(value).lower()})
            elif isinstance(value, list):
                value = map(lambda x: str(x).lower(), value)
                search = search.filter("terms", **{key: value})
        r = search.execute()
        r = r.to_dict()
        count = r['hits']['total']
        docs = r['hits']['hits']
        result = dict(
                data=docs,
                count=count)

        data = result.get('data', {})
        for each in data:
            source = each.pop('_source')
            each['fields'] = source
        return data

    def update(self, instances):
        self.set_attr('update')
        return self.es_indexer.sync(docs=instances)

    def delete(self, instances):
        self.set_attr('delete')
        return self.es_indexer.sync(docs=instances)
