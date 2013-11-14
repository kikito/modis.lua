modis.lua
==========

This Lua library allows using (a subset of) the Mongo Query Language over redis. Effectively, this means that redis can be used as a document-based database.

Interface
=========

    local modis = require 'modis'
    local redis  = require 'redis'

    local red = <initialize redis connection>

    local conn = modis.connect(red)
    local db = conn.getDatabase('test')

    -- create two documents in the collection 'users'
    db.users.insert({name = 'peter', age = 52, groups = {'bowling', 'pub'} })
    db.users.insert({name = 'megan', age = 17, groups = {'ignored'} })

    -- find all users
    -- note that find returns an cursor
    local all_users = db.users.find({})

    -- find the users by name
    -- findOne returns the document
    local peter = db.users.findOne({name = 'peter'})

    -- find users by other fields
    local megan      = db.users.find({age = {['$lt'] = 17 }})[1]
    local also_megan = db.users.find({groups = {['$in'] = {'ignored'}}})[1]
    local megan_too  = db.users.findOne({name = 'megan'}) -- findOne does not require [1]

    -- Iterate over a cursor with forEach
    db.users:find():forEach(function(user)
      print(user.name)
    end)

    -- Iterate over a cursor with next + hasNext
    local cursor = db.users:find()
    while cursor:hasNext() do
      print(cursor:next().name)
    end

    -- Iterate over a cursor with numbers
    local cursor = db.users:find()
    for i=1,cursor:count() do
      print(cursor[i].name)
    end

    -- Update all users and add a new attribute
    db.users:update({}, {stress = 'high'})

    -- Update only some users
    db.users:update({name='peter'}, {stress = 'low'})


    -- You don't need to close db, but you need to close red
    red:close()

Installation
============

Just copy modis.lua in your environment.

Dependencies
============

Your environment must provide a way to connect to redis, but this module does not depend on any particular implementation of redis.

This module requires luajson to be available via `require 'luajson'`

Specs
=====

This project uses [busted](http://olivinelabs.com/busted/) for its specs. In order to run the specs,

    cd path/to/container/of/specs
    busted

License
=======

This library is distributed under the MIT License



