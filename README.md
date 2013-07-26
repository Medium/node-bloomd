node-bloomd
===========

A NodeJS client for [Bloomd](https://github.com/armon/bloomd)

Features
--------

* Complete support for all Bloomd's commands.
* Fast performance: insertion of 250k items in around 500ms on a 2010 MBP.
* Familiar interface, similar to node-redis

Install
-------

    npm install bloomd

Requirements
------------

node-bloomd uses stream transforms, and therefore requires Node 0.10 or later.

Usage
-----

Create a client, then call bloomd commands directly on it. A simple example:


```js
    var bloomd = require('./index')
        client = bloomd.createClient()
    
    client.on('error', function (err) {
        console.log('Error:' + err)
    })  
    
    client.list(null, bloomd.print)
    client.create('newFilter', bloomd.print)
    client.info('newFilter', bloomd.print)
    client.check('newFilter', 'monkey', bloomd.print)
    client.set('newFilter', 'monkey', bloomd.print)
    client.check('newFilter', 'monkey', bloomd.print)
    client.bulk('newFilter', ['monkey', 'magic', 'muppet'], bloomd.print)
    client.multi('newFilter', ['monkey', 'magic', 'muppet'], bloomd.print)
    client.info('newFilter', bloomd.print)
    client.drop('newFilter', bloomd.print)
    client.dispose()
```

Still To Do
-----------

* Offline command Buffering for dropped connections and early requests.
* Partial list caching, to avoid re-checking.
* Retry and reconnect support.
* More Error checking.
* Additional tests.
* Instrumentation and optimisation.
* Better documentation.

Contributions
-------------

Questions, comments, bug reports and pull requests are all welcomed.

In particular, improvements that address any of the tasks on the above
list would be great.

Author
------

[Jamie Talbot](https://github.com/majelbstoat), supported by
[Medium](https://medium.com).

License
-------

Copyright 2013 [Medium](https://medium.com)

Licensed under Apache License Version 2.0.  Details in the attached LICENSE
file.

