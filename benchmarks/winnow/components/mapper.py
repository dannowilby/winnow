from msgpack import packb, unpackb
import re

import wit_world

class WitWorld(wit_world.WitWorld):
    def map_fn(self, key, value):

        raw = unpackb(value)

        words = set(re.findall(r"\b[a-zA-Z]+\b", raw))

        output = list()
        for word in words:
            output.append((word, packb(1)))

        return output