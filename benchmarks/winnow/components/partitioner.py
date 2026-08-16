import wit_world

class WitWorld(wit_world.WitWorld):
    def partition_fn(self, key, r):
        return str(hash(key) % r)