.. _getting_started:

Getting Started
===============

redis-completion was designed to make type-ahead search easy to integrate with
your existing app.  Assuming you have followed the :ref:`installation notes <installing>`,
lets get started building a simple stock symbol lookup.  I've gone ahead and uploaded
a list of symbols and company names, which can be found here:

http://media.charlesleifer.com/downloads/misc/NYSE.txt

Let's get started by writing a script to populate our index by pulling the file
down, reading its contents, then storing a mapping of company name -> symbol so
we can easily search for companies we're interested in.

.. code-block:: python

    import urllib2
    from redis_completion import RedisEngine

    engine = RedisEngine(prefix='stocks')

    def load_data():
        url = 'http://media.charlesleifer.com/downloads/misc/NYSE.txt'
        contents = urllib2.urlopen(url).read()
        for row in contents.splitlines()[1:]:
            ticker, company = row.split('\t')
            engine.store_json(ticker, company, {'ticker': ticker, 'company': company}) # id, search phrase, data

    def search(p, **kwargs):
        return engine.search_json(p, **kwargs)

Save this script to a file and open up an interactive shell:

.. code-block:: python

    >>> from stocks import *
    >>> load_data() # this may take a few seconds

Excellent, we've loaded all the data and can now perform searches on it:

.. code-block:: python

    >>> search('uni sta')
    [{u'company': u'Strats Sm Trust For United States Cellular Corp',
      u'ticker': u'GJH'},
     {u'company': u'United States Cellular Corp.', u'ticker': u'USM'},
     {u'company': u'United States Cellular Corp.', u'ticker': u'UZA'},
     {u'company': u'United States Steel Corp.', u'ticker': u'X'}]

    >>> search('shi co')
    [{u'company': u'International Shipholding Corp.', u'ticker': u'ISH'},
     {u'company': u'Shinhan Financial Group Co Ltd', u'ticker': u'SHG'},
     {u'company': u'Teekay Shipping Corp.', u'ticker': u'TK'}]

    >>> search('prog')
    [{u'company': u'Progress Energy Inc.', u'ticker': u'PGN'},
     {u'company': u'Progressive Corp.', u'ticker': u'PGR'}]
