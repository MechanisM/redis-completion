try:
    import simplejson as json
except ImportError:
    import json
import re

from redis import Redis

from redis_completion.stop_words import STOP_WORDS as _STOP_WORDS
from redis_completion.text import create_key as _ck, partial_complete as _pc


# aggressive stop words will be better when the length of the document is longer
AGGRESSIVE_STOP_WORDS = _STOP_WORDS

# default stop words should work fine for titles and things like that
DEFAULT_STOP_WORDS = set(['a', 'an', 'of', 'the'])

DEFAULT_GRAM_LENGTHS = (2, 3)


class RedisEngine(object):
    """
    References
    ----------

    http://antirez.com/post/autocomplete-with-redis.html
    http://stackoverflow.com/questions/1958005/redis-autocomplete/1966188#1966188
    """
    def __init__(self, gram_lengths=None, min_length=3, prefix='ac', stop_words=None, terminator='^', **conn_kwargs):
        self.conn_kwargs = conn_kwargs
        self.client = self.get_client()

        gl = (gram_lengths is None) and DEFAULT_GRAM_LENGTHS or gram_lengths
        assert len(gl) == 2, 'gram_lengths must be a 2-tuple'
        self.min_words, self.max_words = gl

        self.min_length = min_length
        self.prefix = prefix
        self.stop_words = (stop_words is None) and DEFAULT_STOP_WORDS or stop_words
        self.terminator = terminator

        self.data_key = lambda k: '%s:d:%s' % (self.prefix, k)
        self.members_key = lambda k: '%s:m:%s' % (self.prefix, k)
        self.search_key = lambda k: '%s:s:%s' % (self.prefix, k)
        self.title_key = lambda k: '%s:t:%s' % (self.prefix, k)

    def get_client(self):
        return Redis(**self.conn_kwargs)

    def flush(self, everything=False, batch_size=1000):
        if everything:
            return self.client.flushdb()

        # this could be expensive :-(
        keys = self.client.keys('%s*' % self.prefix)

        # batch keys
        for i in range(0, len(keys), batch_size):
            self.client.delete(*keys[i:i+batch_size])

    def score_key(self, k, max_size=10):
        k_len = len(k)
        iters = min(max_size, k_len)
        a = ord('a') - 1
        score = 0

        for i in range(iters):
            c = (ord(k[i]) - a)
            score += c*(26**(iters-i))
        return score

    def create_key(self, phrase):
        return _ck(phrase, self.max_words, self.stop_words)

    def partial_complete(self, phrase):
        return _pc(phrase, self.min_words, self.max_words, self.stop_words)

    def autocomplete_keys(self, phrase):
        key = self.create_key(phrase)
        ml = self.min_length

        for i, char in enumerate(key[ml:]):
            yield (key[:i+ml], char, ord(char))

        yield (key, self.terminator, 0)

    def store(self, obj_id, title=None, data=None):
        pipe = self.client.pipeline()

        if title is None:
            title = obj_id
        if data is None:
            data = title

        title_score = self.score_key(self.create_key(title))

        pipe.set(self.data_key(obj_id), data)
        pipe.set(self.title_key(obj_id), title)

        # create tries using sorted sets and add obj_data to the lookup set
        for partial_title in self.partial_complete(title):
            # store a reference to our object in the lookup set
            partial_key = self.create_key(partial_title)
            pipe.zadd(self.members_key(partial_key), obj_id, title_score)

            for (key, value, score) in self.autocomplete_keys(partial_title):
                pipe.zadd(self.search_key(key), value, score)

        pipe.execute()

    def store_json(self, obj_id, title, data_dict):
        return self.store(obj_id, title, json.dumps(data_dict))

    def remove(self, obj_id):
        obj_id = str(obj_id)
        title = self.client.get(self.title_key(obj_id)) or ''
        keys = []

        #...how to figure out if its the final item...
        for partial_title in self.partial_complete(title):
            # get a list of all the keys that would have been set for the tries
            autocomplete_keys = list(self.autocomplete_keys(partial_title))

            # flag for whether ours is the last object at this lookup
            is_last = False

            # grab all the members of this lookup set
            partial_key = self.create_key(partial_title)
            set_key = self.members_key(partial_key)
            objects_at_key = self.client.zrange(set_key, 0, -1)

            # check the data at this lookup set to see if ours was the only obj
            # referenced at this point
            if obj_id not in objects_at_key:
                # something weird happened and our data isn't even here
                continue
            elif len(objects_at_key) == 1:
                # only one object stored here, remove the terminal flag
                zset_key = self.search_key(partial_key)
                self.client.zrem(zset_key, '^')

                # see if there are any other references to keys here
                is_last = self.client.zcard(zset_key) == 0

            if is_last:
                for (key, value, score) in reversed(autocomplete_keys):
                    key = self.search_key(key)

                    # another lookup ends here, so bail
                    if '^' in self.client.zrange(key, 0, 1):
                        self.client.zrem(key, value)
                        break
                    else:
                        self.client.delete(key)

                # we can just blow away the lookup key
                self.client.delete(set_key)
            else:
                # remove only our object's data
                self.client.zrem(set_key, obj_id)

        # finally, remove the data from the data key
        self.client.delete(self.data_key(obj_id))
        self.client.delete(self.title_key(obj_id))

    def search(self, phrase, limit=None, filters=None, mappers=None):
        """
        Wrap our search & results with prefixing
        """
        phrase = self.create_key(phrase)

        # perform the depth-first search over the sorted sets
        results = self._search(self.search_key(phrase), limit)

        # strip the prefix off the keys that indicated they matched a lookup
        prefix_len = len(self.prefix) + 3 # 3 becuase ':x:'
        cleaned_keys = map(lambda x: self.members_key(x[prefix_len:]), results)

        # lookup the data references for each lookup set
        obj_ids = []
        for key in cleaned_keys:
            obj_ids.extend(self.client.zrange(key, 0, -1, withscores=True))

        obj_ids.sort(key=lambda i: i[1])

        seen = set()
        ct = 0
        data = []

        # grab the data for each object
        for lookup, _ in obj_ids:
            if lookup in seen:
                continue

            seen.add(lookup)

            raw_data = self.client.get(self.data_key(lookup))
            if not raw_data:
                continue

            if mappers:
                for m in mappers:
                    raw_data = m(raw_data)

            if filters:
                passes = True
                for f in filters:
                    if not f(raw_data):
                        passes = False
                        break

                if not passes:
                    continue

            data.append(raw_data)
            ct += 1
            if limit and ct == limit:
                break

        return data

    def search_json(self, phrase, limit=None, filters=None, mappers=None):
        if not mappers:
            mappers = []
        mappers.insert(0, json.loads)
        return self.search(phrase, limit, filters, mappers)

    def _search(self, text, limit):
        w = []

        for char in self.client.zrange(text, 0, -1):
            if char == self.terminator:
                w.append(text)
            else:
                w.extend(self._search(text + char, limit))

            if limit and len(w) >= limit:
                return w[:limit]

        return w
