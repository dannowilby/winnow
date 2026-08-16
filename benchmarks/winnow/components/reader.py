from msgpack import packb

import wit_world

class WitWorld(wit_world.WitWorld):
    def read_fn(self, key):
        segs = key.split("-")

        source = segs[0]
        offset = int(segs[1]) if len(segs) > 1 else 0
        length = 64000000

        with open(source, "rb") as fd:
            fd.seek(offset)
            chunk = fd.read(length)

        return packb(chunk.decode("utf-8", errors="replace"))