import json


class CreateIndex(object):

    def __init__(self, es, config, index, doc_type, max_limit, model, *args, **kwargs):
        self.es = es
        self.config = config
        self.index = index
        self.doc_type = doc_type
        self.max_limit = max_limit
        self.args = args
        self.kwargs = kwargs
        self.model = model

    def create(self):
        body = self.config['AUTOCOMPLETE_INDEX_SETTING']
        mapping = self.config['AUTOCOMPLETE_INDEX_MAPPING']
        es = self.es
        es.indices.create(
            index=self.index,
            body=json.dumps(body))
        es.indices.put_mapping(
            index=self.index,
            doc_type=self.doc_type,
            body=json.dumps(mapping))

