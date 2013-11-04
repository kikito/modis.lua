local modis = {
  _VERSION     = 'modis v0.0.1',
  _DESCRIPTION = 'Mongo Query Language on top of redis for Lua',
  _LICENSE     = [[
    MIT LICENSE

    Copyright (c) 2011 Enrique García Cota, Robin Wellner

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

-- This is a modification of ser - https://github.com/gvx/Ser, by Robin Wellner
local ser = (function()
  local concat, floor = table.concat, math.floor

  local function getchr(c) return "\\" .. c:byte() end
  local function make_safe(text)
    return ("%q"):format(text):gsub('\n', 'n'):gsub("[\128-\255]", getchr)
  end

  local oddvals = {inf = '1/0', ['-inf'] = '-1/0', ['nan'] = '0/0'}
  local function write(t, memo, rev_memo)
    local ty = type(t)
    if ty == 'number' or ty == 'boolean' or ty == 'nil' then
      t = tostring(t)
      return oddvals[t] or t
    elseif ty == 'string' then
      return make_safe(t)
    elseif ty == 'table' then
      if not memo[t] then
        local index = #rev_memo + 1
        memo[t] = index
        rev_memo[index] = t
      end
      return '_' .. memo[t]
    else
      error("Trying to serialize unsupported type " .. ty)
    end
  end

  local keywords = {}
  for word in ([[
    and break do else elseif end false for function goto if
    in local nil not or repeat return then true until while
  ]]):gmatch('%w+') do keywords[word] = true end

  local function write_key_value_pair(k, v, memo, rev_memo, name)
    if type(k) == 'string' and k:match '^[_%a][_%w]*$' and not keywords[k] then
      return (name and name .. '.' or '') .. k ..' = ' .. write(v, memo, rev_memo)
    else
      return (name or '') .. '[' .. write(k, memo, rev_memo) .. '] = ' .. write(v, memo, rev_memo)
    end
  end

  -- fun fact: this function is not perfect
  -- it has a few false positives sometimes
  -- but no false negatives, so that's good
  local function is_cyclic(memo, sub, super)
    local m, p = memo[sub], memo[super]
    return m and p and m < p
  end

  local function write_table_ex(t, memo, rev_memo, srefs, name)
    local m = {'local _', name, ' = {'}
    local mi = 3
    for i = 1, #t do -- don't use ipairs here, we need the gaps
      local v = t[i]
      if v == t or is_cyclic(memo, v, t) then
        srefs[#srefs + 1] = {name, i, v}
        m[mi + 1] = 'nil, '
        mi = mi + 1
      else
        m[mi + 1] = write(v, memo, rev_memo)
        m[mi + 2] = ', '
        mi = mi + 2
      end
    end
    for k,v in pairs(t) do
      if type(k) ~= 'number' or floor(k) ~= k or k < 1 or k > #t then
        if v == t or k == t or is_cyclic(memo, v, t) or is_cyclic(memo, k, t) then
          srefs[#srefs + 1] = {name, k, v}
        else
          m[mi + 1] = write_key_value_pair(k, v, memo, rev_memo)
          m[mi + 2] = ', '
          mi = mi + 2
        end
      end
    end
    m[mi > 3 and mi or mi + 1] = '}'
    return concat(m)
  end

  return function(t)
    local srefs, result, n, memo, rev_memo = {}, {}, 0, {[t] = 0}, {[0] = t}

    -- phase 1: recursively descend the table structure
    while rev_memo[n] do
      result[n + 1] = write_table_ex(rev_memo[n], memo, rev_memo, srefs, n)
      n = n + 1
    end

    -- phase 2: reverse order
    for i = 1, n*.5 do
      local j = n - i + 1
      result[i], result[j] = result[j], result[i]
    end

    -- phase 3: add all the tricky cyclic stuff
    for i, v in ipairs(srefs) do
      n = n + 1
      result[n] = write_key_value_pair(v[2], v[3], memo, rev_memo, '_' .. v[1])
    end

    -- phase 4: add something about returning the main table
    if result[n]:sub(1, 8) == 'local _0' then
      result[n] = 'return' .. result[n]:sub(11)
    else
      result[n + 1] = 'return _0'
    end

    -- phase 5: just concatenate everything
    return concat(result, '\n')
  end

end)()

local serialize   = function(obj)
  return ser(obj)
end
local deserialize = function(str)
  return assert(loadstring(str))()
end

local function table_merge(dest, source)
  if type(source) ~= 'table' then return source end

  dest = dest or {}
  for k,v in pairs(source) do
    assert(type(k) == 'string' or type(k) == 'number', 'key must be a string or number')
    dest[k] = table_merge(dest[k], v)
  end
  return dest
end

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
  assert(db.red:sadd(db.name .. '/cols', self.name))
  db.collections[self.name] = self
end

function Collection:exists()
  local db = self.db
  return db.red:sismember(db.name .. '/cols', self.name)
end

function Collection:count()
  local db = self.db
  return db.red:scard(db.name .. '/cols/' .. self.name .. '/items')
end

function Collection:insert(doc)
  markAsExisting(self)
  local db = self.db
  local new_doc = table_merge({}, doc)

  local id = self:count() + 1
  new_doc._id = new_doc._id or id

  db.red:sadd(db.name .. '/cols/' .. self.name .. '/items', id)
  db.red:set(db.name .. '/cols/' .. self.name .. '/items/' .. tostring(id), serialize(new_doc))
  return new_doc
end

function Collection:find(criteria)
  local db = self.db
  local items = {}
  for i=1,self:count() do
    if not criteria._id or criteria._id == i then
      local serialized = db.red:get(db.name .. '/cols/' .. self.name .. '/items/' .. tostring(i))
      items[#items + 1] = deserialize(serialized)
    end
  end
  return items
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
  ]]):format(self.name)
  return self.red:eval(script, 0)
end

function DB:getCollectionNames()
  local names = assert(self.red:smembers(self.name .. '/cols'))
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
    red       = red,
    name      = options.name or 'modis',
    collections = {}
  }, DB_mt)
end

return modis
