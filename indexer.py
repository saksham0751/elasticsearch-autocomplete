from elasticsearch import helpers


def expand_action(data, **kwargs):
    # make sure we don't alter the action
    data = data.copy()
    op_type = getattr(expand_action, 'op_type', 'index')
    doc_type = getattr(expand_action, 'doc_type', None)
    model_name = getattr(expand_action, 'model_name', None)
    callbacks = getattr(expand_action, 'callbacks', [])
    extra = getattr(expand_action, 'extra', {})
    app = expand_action.app
    action = {op_type: {}}
    for key in (
            '_index', '_parent', '_percolate', '_routing', '_timestamp',
            '_ttl', '_type', '_version', '_id', '_retry_on_conflict'):
        if key in data:
            action[op_type][key] = data.pop(key)

    action[op_type]['_id'] = data.get('id')

    for callback in callbacks:
        f = callback.split('.')
        method = getattr(
            __import__('.'.join(f[:2]), fromlist=[f[2]]), f[2])
        try:
            method(app, model_name, data, **extra)
        except IndexError:
            # eat it
            pass

    # no data payload for delete
    if op_type == 'delete':
        payload = None
    elif op_type == 'update':
        payload = {'doc': data.get('_source', data)}
    else:
        payload = data.get('_source', data)

    return action, payload


class ESIndexer(object):

    def __init__(self, es, *args, **kwargs):
        self.docs = []
        self.es = es
        self.doc_type = kwargs.get('doc_type')
        self.index = kwargs.pop('index', None)
        self.body = kwargs.pop('body', {})
        self.op_type = kwargs.get('op_type', 'index')
        self.helpers = helpers
        self.fields = kwargs.pop('fields', None)
        self.app = kwargs.pop('app', None)

    def sync(self, docs=None, **kwargs):

        expand_action.doc_type = self.doc_type
        expand_action.op_type = self.op_type
        expand_action.app = self.app
        expand_action.index = self.index
        if docs is not None and isinstance(docs, list):
            self.docs = self._get_list_of_dict(docs)
            self.helpers.bulk(
                self.es, actions=self.docs,
                index=self.index, doc_type=self.doc_type,
                expand_action_callback=expand_action)

    def _get_list_of_dict(self, docs):
        data_list = []
        for each in docs:
            data_dict = {}
            for field in self.fields:
                data_dict[field] = getattr(each, field)
            data_list.append(data_dict)
        return data_list

