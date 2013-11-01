local modis = {
  _VERSION     = 'modis v0.0.1',
  _DESCRIPTION = 'Mongo Query Language on top of redis for Lua',
  _LICENSE     = [[
    MIT LICENSE

    Copyright (c) 2011 Enrique García Cota

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
    OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
    CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
    SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
  ]]
}

local Collection = {}
local Collection_mt = {__index = Collection}

local function newCollection(db, name)
  return setmetatable({
    db = db,
    name = name
  }, Collection_mt)
end

local function markAsExisting(self)
  local db = self.db
  assert(db.red:sadd(db.prefix .. '/cols', self.name))
  db.collections[self.name] = self
end

function Collection:exists()
  local db = self.db
  return db.red:sismember(db.prefix .. '/cols', self.name)
end

function Collection:count()
  local db = self.db
  return db.red:scard(db.prefix .. '/cols/' .. self.name .. '/items')
end

function Collection:insert(doc)
  markAsExisting(self)
  local db = self.db
  local id = self:count() + 1
  return db.red:sadd(db.prefix .. '/cols/' .. self.name .. '/items', id)
end


----------------------------------------------------------

local DB = {}
local DB_mt = {
  __index = function(self, key)
    if DB[key] then return DB[key] end
    return self:getCollection(key)
  end
}

function DB:dropDatabase()
  local script = ([[
    for _,k in ipairs(redis.call('keys', '%s/*')) do
      redis.call('del', k)
    end
  ]]):format(self.prefix)
  return self.red:eval(script, 0)
end

function DB:getCollectionNames()
  local names = assert(self.red:smembers(self.prefix .. '/cols'))
  table.sort(names)
  return names
end

function DB:createCollection(collection)
  local col = self:getCollection(collection)
  markAsExisting(col)
  return col
end

function DB:getCollection(collection)
  return self.collections[collection] or newCollection(self, collection)
end

----------------------------------------------------------

function modis.connect(red, options)
  options = options or {}
  return setmetatable({
    red         = red,
    prefix      = options.prefix or 'modis',
    collections = {}
  }, DB_mt)
end

return modis
