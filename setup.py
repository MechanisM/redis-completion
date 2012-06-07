import os
from setuptools import setup, find_packages


setup(
    name='redis-completion',
    version="0.2.0",
    description='autocomplete with redis',
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/redis-completion/tree/master',
    packages=find_packages(),
    package_data = {
        'redis_completion': [
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='runtests.runtests',
)
