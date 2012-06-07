try:
    import simplejson as json
except ImportError:
    import json
import re

from redis import Redis

from redis_completion.stop_words import STOP_WORDS as _STOP_WORDS


# aggressive stop words will be better when the length of the document is longer
AGGRESSIVE_STOP_WORDS = _STOP_WORDS

# default stop words should work fine for titles and things like that
DEFAULT_STOP_WORDS = set(['a', 'an', 'of', 'the'])


class RedisEngine(object):
    """
    References
    ----------

    http://antirez.com/post/autocomplete-with-redis.html
    http://stackoverflow.com/questions/1958005/redis-autocomplete/1966188#1966188
    http://patshaughnessy.net/2011/11/29/two-ways-of-using-redis-to-build-a-nosql-autocomplete-search-index
    """
    def __init__(self, min_length=2, prefix='ac', stop_words=None, cache_timeout=300, **conn_kwargs):
        self.conn_kwargs = conn_kwargs
        self.client = self.get_client()

        self.min_length = min_length
        self.prefix = prefix
        self.stop_words = (stop_words is None) and DEFAULT_STOP_WORDS or stop_words
        self.cache_timeout = cache_timeout

        self.data_key = '%s:d' % self.prefix
        self.title_key = '%s:t' % self.prefix
        self.search_key = lambda k: '%s:s:%s' % (self.prefix, k)

    def get_client(self):
        return Redis(**self.conn_kwargs)

    def flush(self, everything=False, batch_size=1000):
        if everything:
            return self.client.flushdb()

        # this could be expensive :-(
        keys = self.client.keys('%s:*' % self.prefix)

        # batch keys
        for i in range(0, len(keys), batch_size):
            self.client.delete(*keys[i:i+batch_size])

    def score_key(self, k, max_size=20):
        k_len = len(k)
        a = ord('a') - 2
        score = 0

        for i in range(max_size):
            if i < k_len:
                c = (ord(k[i]) - a)
                if c < 2 or c > 27:
                    c = 1
            else:
                c = 1
            score += c*(27**(max_size-i))
        return score

    def clean_phrase(self, phrase):
        phrase = re.sub('[^a-z0-9_\-\s]', '', phrase.lower())
        return [w for w in phrase.split() if w not in self.stop_words]

    def create_key(self, phrase):
        return ' '.join(self.clean_phrase(phrase))

    def autocomplete_keys(self, w):
        ml = self.min_length
        for i, char in enumerate(w[ml:]):
            yield w[:i+ml]
        yield w

    def store(self, obj_id, title=None, data=None):
        pipe = self.client.pipeline()

        if title is None:
            title = obj_id
        if data is None:
            data = title

        title_score = self.score_key(self.create_key(title))

        pipe.hset(self.data_key, obj_id, data)
        pipe.hset(self.title_key, obj_id, title)

        for word in self.clean_phrase(title):
            for partial_key in self.autocomplete_keys(word):
                pipe.zadd(self.search_key(partial_key), obj_id, title_score)

        pipe.execute()

    def store_json(self, obj_id, title, data_dict):
        return self.store(obj_id, title, json.dumps(data_dict))

    def remove(self, obj_id):
        obj_id = str(obj_id)
        title = self.client.hget(self.title_key, obj_id) or ''
        keys = []

        for word in self.clean_phrase(title):
            for partial_key in self.autocomplete_keys(word):
                key = self.search_key(partial_key)
                if not self.client.zrange(key, 1, 2):
                    self.client.delete(key)
                else:
                    self.client.zrem(key, obj_id)

        # finally, remove the data from the data key
        self.client.hdel(self.data_key, obj_id)
        self.client.hdel(self.title_key, obj_id)

    def search(self, phrase, limit=None, filters=None, mappers=None):
        """
        Wrap our search & results with prefixing
        """
        cleaned = self.clean_phrase(phrase)
        if not cleaned:
            return []

        new_key = self.search_key('|'.join(cleaned))
        if not self.client.exists(new_key):
            self.client.zinterstore(new_key, map(self.search_key, cleaned))
            self.client.expire(new_key, self.cache_timeout)

        ct = 0
        data = []

        # grab the data for each object
        for obj_id in self.client.zrange(new_key, 0, -1):
            raw_data = self.client.hget(self.data_key, obj_id)
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
