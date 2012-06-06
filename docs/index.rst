.. redis-completion documentation master file, created by
   sphinx-quickstart on Wed Jun  6 14:51:55 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

redis-completion
================

autocompletion with `redis <http://redis.io>`_ based on:

* http://antirez.com/post/autocomplete-with-redis.html
* http://stackoverflow.com/questions/1958005/redis-autocomplete/1966188


usage
-----

If you just want to store really simple things, like strings:

.. code-block:: python

    engine = RedisEngine()
    titles = ['python programming', 'programming c', 'unit testing python',
              'testing software', 'software design']
    map(engine.store, titles)

    >>> engine.search('pyt')
    ['python programming', 'unit testing python']

    >>> engine.search('test')
    ['testing software', 'unit testing python']


If you want to store more complex data, like blog entries:

.. code-block:: python

    Entry.create(title='an entry about python', published=True)
    Entry.create(title='all about redis', published=True)
    Entry.create(title='using redis with python', published=False)

    for entry in Entry.select():
        engine.store_json(entry.id, entry.title, {
            'published': entry.published,
            'title': entry.title,
            'url': entry.get_absolute_url(),
        })

    >>> engine.search_json('pytho')
    [{'published': True, 'title': 'an entry about python', 'url': '/blog/1/'},
     {'published': False, 'title': 'using redis with python', 'url': '/blog/3/'}]

    # just published entries, please
    >>> engine.search_json('redis', filters=[lambda i: i['published'] == True])
    [{u'published': True, u'title': u'all about redis', u'url': u'/blog/2/'}]


Contents:

.. toctree::
   :maxdepth: 2

   installing
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

