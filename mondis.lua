local mondis = {
  _VERSION     = 'mondis v0.0.1',
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

function Collection:exists()
  return self.db.red:sismember(self.db.prefix .. '/cols', self.name)
end

----------------------------------------------------------

local DB = {}
local DB_mt = {
  __index = function(self, method)
    if DB[method] then return DB[method] end
    return self:getCollection(method)
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
  assert(self.red:sadd(self.prefix .. '/cols', collection))
  self.collections[collection] = self.collections[collection] or newCollection(self, collection)
  return self.collections[collection]
end

function DB:getCollection(collection)
  return self.collections[collection] or newCollection(self, collection)
end

----------------------------------------------------------

function mondis.connect(red, options)
  options = options or {}
  return setmetatable({
    red         = red,
    prefix      = options.prefix or 'mondis',
    collections = {}
  }, DB_mt)
end

return mondis
