from msgpack import packb, unpackb

import wit_world

class WitWorld(wit_world.WitWorld):
    def reduce_fn(self, key, value, acc):

        if len(acc) == 0:
            acc = 0
        else:
            acc = unpackb(acc)

        acc += 1
        
        return packb(acc)