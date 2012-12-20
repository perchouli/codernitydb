from CodernityDB.database import Database
from CodernityDB.sharded_hash import ShardedUniqueHashIndex, ShardedHashIndex
from CodernityDB.hash_index import HashIndex

import random


class CustomIdSharded(ShardedUniqueHashIndex):

    custom_header = 'from CodernityDB.sharded_hash import ShardedUniqueHashIndex'

    def __init__(self, *args, **kwargs):
        kwargs['sh_nums'] = 10
        super(CustomIdSharded, self).__init__(*args, **kwargs)


class MySharded(ShardedHashIndex):

    custom_header = 'from CodernityDB.sharded_hash import ShardedHashIndex'

    def __init__(self, *args, **kwargs):
        kwargs['sh_nums'] = 10
        kwargs['key_format'] = 'I'
        kwargs['use_make_keys'] = True
        super(MySharded, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return data['x'], None

    def calculate_shard(self, key):
        return key % self.sh_nums


y = 1500 * 'y'

db = Database('/tmp/shard')

db.create(with_id_index=False)
db.add_index(CustomIdSharded(db.path, 'id'))
db.add_index(MySharded(db.path, 'x'))


# it makes non sense to use sharding with such small number of records
for x in xrange(10 ** 4):
    db.insert({'x': x, 'y': y})


print db.get('x', random.randint(0, 10 ** 4))['_id']
