import random
from unittest import TestCase

from redis_completion.engine import RedisEngine


stop_words = set(['a', 'an', 'the', 'of'])

class RedisCompletionTestCase(TestCase):
    def setUp(self):
        self.engine = RedisEngine(prefix='testac', db=15)
        self.engine.flush()

    def store_data(self, id=None):
        test_data = (
            (1, 'testing python'),
            (2, 'testing python code'),
            (3, 'web testing python code'),
            (4, 'unit tests with python'),
        )
        for obj_id, title in test_data:
            if id is None or id == obj_id:
                self.engine.store_json(obj_id, title, {
                    'obj_id': obj_id,
                    'title': title,
                    'secret': obj_id % 2 == 0 and 'derp' or 'herp',
                })

    def sort_results(self, r):
        return sorted(r, key=lambda i:i['obj_id'])

    def test_search(self):
        self.store_data()

        results = self.engine.search_json('testing python')
        self.assertEqual(self.sort_results(results), [
            {'obj_id': 1, 'title': 'testing python', 'secret': 'herp'},
            {'obj_id': 2, 'title': 'testing python code', 'secret': 'derp'},
            {'obj_id': 3, 'title': 'web testing python code', 'secret': 'herp'},
        ])

        results = self.engine.search_json('test')
        self.assertEqual(self.sort_results(results), [
            {'obj_id': 1, 'title': 'testing python', 'secret': 'herp'},
            {'obj_id': 2, 'title': 'testing python code', 'secret': 'derp'},
            {'obj_id': 3, 'title': 'web testing python code', 'secret': 'herp'},
            {'obj_id': 4, 'title': 'unit tests with python', 'secret': 'derp'},
        ])

        results = self.engine.search_json('unit')
        self.assertEqual(results, [
            {'obj_id': 4, 'title': 'unit tests with python', 'secret': 'derp'},
        ])

        results = self.engine.search_json('')
        self.assertEqual(results, [])

        results = self.engine.search_json('missing')
        self.assertEqual(results, [])

    def test_limit(self):
        self.store_data()

        results = self.engine.search_json('testing', limit=1)
        self.assertEqual(results, [
            {'obj_id': 1, 'title': 'testing python', 'secret': 'herp'},
        ])

    def test_filters(self):
        self.store_data()

        f = lambda i: i['secret'] == 'herp'
        results = self.engine.search_json('testing python', filters=[f])

        self.assertEqual(self.sort_results(results), [
            {'obj_id': 1, 'title': 'testing python', 'secret': 'herp'},
            {'obj_id': 3, 'title': 'web testing python code', 'secret': 'herp'},
        ])

    def test_simple(self):
        self.engine.print_scores = True
        self.engine.store('testing python')
        self.engine.store('testing python code')
        self.engine.store('web testing python code')
        self.engine.store('unit tests with python')

        results = self.engine.search('testing')
        self.assertEqual(results, ['testing python', 'testing python code', 'web testing python code'])

        results = self.engine.search('code')
        self.assertEqual(results, ['testing python code', 'web testing python code'])

    def test_correct_sorting(self):
        strings = []
        for i in range(26):
            strings.append('aaaa%s' % chr(i + ord('a')))
            if i > 0:
                strings.append('aaa%sa' % chr(i + ord('a')))

        random.shuffle(strings)

        for s in strings:
            self.engine.store(s)

        results = self.engine.search('aaa')
        self.assertEqual(results, sorted(strings))

        results = self.engine.search('aaa', limit=30)
        self.assertEqual(results, sorted(strings)[:30])

    def test_removing_objects(self):
        self.store_data()

        self.engine.remove(1)

        results = self.engine.search_json('testing')
        self.assertEqual(self.sort_results(results), [
            {'obj_id': 2, 'title': 'testing python code', 'secret': 'derp'},
            {'obj_id': 3, 'title': 'web testing python code', 'secret': 'herp'},
        ])

        self.store_data(1)
        self.engine.remove(2)

        results = self.engine.search_json('testing')
        self.assertEqual(self.sort_results(results), [
            {'obj_id': 1, 'title': 'testing python', 'secret': 'herp'},
            {'obj_id': 3, 'title': 'web testing python code', 'secret': 'herp'},
        ])

    def test_removing_objects_in_depth(self):
        # want to ensure that redis is cleaned up and does not become polluted
        # with spurious keys when objects are removed
        redis_client = self.engine.client
        prefix = self.engine.prefix

        initial_key_count = len(redis_client.keys())

        # store the blog "testing python"
        self.store_data(1)

        # see how many keys we have in the db - check again in a bit
        key_len = len(redis_client.keys())

        self.store_data(2)
        key_len2 = len(redis_client.keys())

        self.assertTrue(key_len != key_len2)
        self.engine.remove(2)

        # back to the original amount of keys
        self.assertEqual(len(redis_client.keys()), key_len)

        self.engine.remove(1)
        self.assertEqual(len(redis_client.keys()), initial_key_count)

    def test_clean_phrase(self):
        self.assertEqual(self.engine.clean_phrase('abc def ghi'), ['abc', 'def', 'ghi'])

        self.assertEqual(self.engine.clean_phrase('a A tHe an a'), [])
        self.assertEqual(self.engine.clean_phrase(''), [])

        self.assertEqual(
            self.engine.clean_phrase('The Best of times, the blurst of times'),
            ['best', 'times', 'blurst', 'times'])
