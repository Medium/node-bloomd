node-bloomd
===========

A NodeJS client for [Bloomd](https://github.com/armon/bloomd)

Features
--------

* Complete support for all Bloomd's commands.
* Fast performance: insertion of 235k items in ~600ms on a 2010 MBP, over localhost.
* Familiar interface, similar to node-redis
* A number of useful extensions over and above bloomd's default behaviour:
  - [set|bulk|check|multi|info]Safe() commands to automatically create a filter if it doesn't exist when running a filter-specific command.
  - Squashing non-existent filter errors on drop.

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
  var bloomd = require('./index'),
      client = bloomd.createClient()

  client.on('error', function (err) {
    console.log('Error:' + err)
  })

  function printer(error, data) {
    console.log(data)
  }

  client.list(null, bloomd.print)
  client.create('newFilter', printer)
  client.info('newFilter', bloomd.print)
  client.check('newFilter', 'monkey', printer) 
  client.set('newFilter', 'monkey', printer)
  client.check('newFilter', 'monkey', printer) 
  client.bulk('newFilter', ['monkey', 'magic', 'muppet'], printer) 
  client.multi('newFilter', ['monkey', 'magic', 'muppet'], printer) 
  client.info('newFilter', bloomd.print)
  client.drop('newFilter', printer) 
  client.dispose() 
```

Client Options
--------------

A number of config options are available for the client:

* ```host [127.0.0.1]```: The host of bloomd to connect to.
* ```port [8673]```: The port to connect on.
* ```debug [false]```: Outputs debug information to the log.
* ```reconnectDelay [160]```: The base amount of time in ms to wait between reconnection attempts. This number is multiplied by the current count of reconnection attempts to give a measure of backoff.
* ```maxConnectionAttempts [0]```: The amount of times to try to get a connection to bloomd, after which the client will declare itself unavailable. 0 means no limit.

Memorable Commands
------------------

Pop quiz: Bulk and Multi - which is used for batch checking, and which is used for batch setting?  I
can never remember either.  node-bloomd helps out by providing two methods to make it explicit:
```multiCheck()``` and ```bulkSet()```. Use them. The maintainers of your code will thank you.

'Safe' Commands
---------------

Typically, when issuing a ```set```, ```check```, ```bulk```, or ```multi``` command,
bloomd will respond with "Filter does not exist" if the filter has not been created.  node-bloomd
provides 'safe' versions of these commands which auto-create the filter in this situation.  These
are ```setSafe()```, ```checkSafe()```, ```bulkSafe()```, and ```multiSafe()```.

The method signatures of these are the same as the non-safe equivalent, with the addition of an optional
createOptions parameter, which can be used to control the configuration of the filter that might be created.

There is overhead to co-ordinating all this (see below), so if you are sure that a filter exists,
you should use the non-safe version of the command.

Subsequent commands issued to the same filter are guaranteed to happen after both the creation command
and the safe command that triggered the creation, even if the filter didn't previously exist. For example:

```js
  var bloomd = require('./index'),
      client = bloomd.createClient()

  client.bulkSafe('nonExistent', ['a', 'b', 'c', 'd'], function(error, data) {
    console.log('First, we created and bulk set some values')
  }, {
    prob: 0.01,
    capacity: 50000
  })

  client.check('nonExistent', 'a', function (error, data) {
    console.log('This will run second, and will be true')
  })
```

In order to do this, when a safe command is issued, subsequent commands on the same filter are held
until we have attempted to create the filter and process the original safe command.

This requires the use of a per-filter sub-queue, which is then processed when both the create command
and the originating command has completed.  While not a huge overhead, it is certainly slower than just
the non-safe version of the command.

In order of speed, from fastest to slowest:

* set().
* setSafe(), where the filter already exists.
* setSafe() on a non-existent filter.

Note that a safe command can still fail if the create method fails. Typically, this happens due to bad
creation parameters, such as too low a capacity being chosen. To aid with debugging, in this instance,
the error passed to the safe command's callback will be the reason that the filter creation failed, not
the reason that the safe command failed (which would be, in all cases "Filter does not exist"). Any
subsequent commands that were also queued will still fail with "Filter does not exist".

Finally, 'safe' is a terrible designation, and I welcome suggestions for a better name.

Still To Do
-----------

* More Error checking.
* Instrumentation and optimisation.
* Better documentation.
* Auto-retry of filter creation when failing due to the filter having recently been dropped.

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

Copyright 2013 [The Obvious Corporation](https://medium.com)

Licensed under Apache License Version 2.0.  Details in the attached LICENSE
file.

