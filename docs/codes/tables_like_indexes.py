class AllUsers(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(AllUsers, self).__init__(*args, **kwargs)

    def make_key(self, key):
        return md5(key).digest()

    def make_key_value(self, data):
        if data.get('_t') == 'user':
            return md5(data.get('name')).digest(), None


class AllItems(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(AllItems, self).__init__(*args, **kwargs)

    def make_key(self, key):
        return md5(key).digest()

    def make_key_value(self, data):
        if data.get('_t') == 'item':
            return md5(data.get('owner')).digest(), None
