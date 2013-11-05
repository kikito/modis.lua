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

local OPERATORS = {
  ['$gt']  = 'comparison',
  ['$gte'] = 'comparison',
  ['$lt']  = 'comparison',
  ['$lte'] = 'comparison',

  ['$in']  = 'collection',
  ['$all'] = 'collection',
  ['$not'] = 'collection'
}

local concat, floor, max, min = table.concat, math.floor, math.max, math.min

local function isEmpty(tbl)
  return next(tbl) == nil
end

local function isArray(array)
  local maximum, count = 0, 0
  for k, _ in pairs(array) do
    if type(k) ~= 'number' or k < 0 or floor(k) ~= k then return false end
    maximum, count = max(maximum, k), count + 1
  end
  return count == maximum
end

-- This is a modification of ser - https://github.com/gvx/Ser, by Robin Wellner
local ser = (function()

  local function escape(c) return "\\" .. c:byte() end
  local function make_safe(text)
    return ("%q"):format(text):gsub('\n', 'n'):gsub("[\128-\255]", escape)
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

local serialize   = function(doc)
  return ser(doc)
end
local deserialize = function(str)
  return assert(loadstring(str))()
end

local function assertIsInstance(self, mt,  method)
  if type(self) ~= 'table' or getmetatable(self) ~= mt then
    error(mt.name .. '.' .. method .. ' was called. Use the ":" -> ' .. mt.name .. ':' .. method)
  end
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

local function array_intersect(arr1, arr2)
  local values_dict = {}
  for _,v in ipairs(arr1) do values_dict[v] = true end
  local result, len = {}, 0
  for _,v in ipairs(arr2) do
    if values_dict[v] then
      len = len + 1
      result[len] = v
    end
  end
  return result
end

local function array_truncate(arr, len)
  local result = {}
  for i=1,min(#arr, len) do result[i] = arr[i] end
  return result
end

----------------------------------------------------------

local function hasOperators(doc)
  for k,_ in pairs(doc) do
    if OPERATORS[k] then return true end
  end
  return false
end

local function recursive_flatten(doc, result, prefix)
  if type(doc) ~= 'table' or isArray(doc) or hasOperators(doc) then
    result[prefix] = doc
  else
    for k,v in pairs(doc) do
      k = prefix and (prefix .. '.' .. tostring(k)) or k
      recursive_flatten(v, result, k)
    end
  end
  return result
end

-- flatten({foo = { bar = 'baz' }}) = {['foo.bar'] = 'baz'}
local function flatten(doc)
  if type(doc) ~= 'table' then error('can not flatten non-tables: ' .. tostring(doc)) end
  if isEmpty(doc)         then return {} end
  if isArray(doc)         then error('can not flatten arrays: {' .. table.concat(doc, ',') .. '}') end
  if hasOperators(doc)    then error('the provided docect had operators (such as $lt) on its first-level keys') end
  return recursive_flatten(doc, {})
end

----------------------------------------------------------
local Collection = {}
local Collection_mt = {__index = Collection, name = 'Collection'}

local function markAsExisting(self)
  local db, red = self.db, self.db.red
  db.collections[self.name] = self
  assert(red:sadd(db.name .. '/cols', self.name))
end

local function removeIndexes(self, id)
  -- note: this function assumes that the id exists
  local db, red = self.db, self.db.red
  local doc = deserialize(red:get(db.name .. '/cols/' .. self.name .. '/items/' .. id))

  local index_prefix = db.name .. '/cols/' .. self. name .. '/index/'
  for index_name, value in pairs(flatten(doc)) do
    local vtype = type(value)
    if     vtype == 'string' then
      red:srem(index_prefix .. 'string?' .. index_name .. '=' .. value, id)
    elseif vtype == 'boolean' then
      red:srem(index_prefix .. 'boolean?' .. index_name .. '=' .. tostring(value), id)
    elseif vtype == 'number' then
      red:zrem(index_prefix .. 'number?' .. index_name, id)
    else
      error('unknown value type for object ' .. self.name .. '/' .. tostring(id) .. '.' .. index_name  .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
    end
  end
end

local function addIndexes(self, id, doc)
  -- note: this function assumes that the id exists
  local db, red = self.db, self.db.red

  local index_prefix = db.name .. '/cols/' .. self. name .. '/index/'
  for index_name, value in pairs(flatten(doc)) do
    local vtype = type(value)
    if     vtype == 'string' then
      red:sadd(index_prefix .. 'string?' .. index_name .. '=' .. value, id)
    elseif vtype == 'boolean' then
      red:sadd(index_prefix .. 'boolean?' .. index_name .. '=' .. tostring(value), id)
    elseif vtype == 'number' then
      red:zadd(index_prefix .. 'number?' .. index_name, value, id)
    else
      error('unknown value type for object ' .. self.name .. '/' .. tostring(id) .. '.' .. index_name  .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
    end
  end

end

local function findIdsMatchingIndex(self, index_name, value)
  local db, red = self.db, self.db.red

  local ids
  local vtype = type(value)

  local index_prefix = db.name .. '/cols/' .. self. name .. '/index/'

  if     vtype == 'string' then
    ids = red:smembers(index_prefix .. 'string?' .. index_name .. '=' .. value)
  elseif vtype == 'boolean' then
    ids = red:smembers(index_prefix .. 'boolean?' .. index_name .. '=' .. tostring(value))
  elseif vtype == 'number' then
    ids = red:zrangebyscore(index_prefix .. 'number?' .. index_name, value, value)
  elseif vtype == 'table' then
    if     isEmpty(value) then
      error('value in ' .. index_name .. ' can not be an empty table')
    elseif isArray(value) then
      ids = find_ids_matching_index(self, index_name, value[1])
      for i = 2, #value do
        if isEmpty(ids) then break end
        ids = array_intersect(ids, find_ids_matching_index(self, index_name, value[i]))
      end
    elseif hasOperators(value) then
      -- FIXME
      return {}
    else
      error ('could not parse table value in ' .. index_name)
    end
  else
    error ('unknown value type in ' .. index_name .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
  end

  return ids
end

local function findIdsMatchingQuery(self, query, limit)
  query = query or {}
  limit = limit or math.huge

  local db, red = self.db, self.db.red
  local ids = red:smembers(db.name .. '/cols/' .. self.name .. '/ids')

  for index_name,value in pairs(flatten(query)) do
    if isEmpty(ids) then break end
    ids = array_intersect(ids, findIdsMatchingIndex(self, index_name, value))
  end

  return array_truncate(ids, limit)
end

function Collection:exists()
  assertIsInstance(self, Collection_mt, 'exists')
  local db, red = self.db, self.db.red
  return red:sismember(db.name .. '/cols', self.name)
end

function Collection:count()
  assertIsInstance(self, Collection_mt, 'count')
  local db, red = self.db, self.db.red
  return red:scard(db.name .. '/cols/' .. self.name .. '/ids')
end

function Collection:insert(doc)
  assertIsInstance(self, Collection_mt, 'insert')
  markAsExisting(self)

  local db, red = self.db, self.db.red
  local docs = isArray(doc) and doc or {doc}

  for _,doc in ipairs(docs) do
    local new_doc = table_merge({}, doc)

    new_doc._id = new_doc._id or self:count() + 1 -- FIXME not thread safe
    local id = new_doc._id

    local is_new = red:sismember(db.name .. '/cols/' .. self.name .. '/ids', id)
    if is_new then removeIndexes(self, id) end

    red:sadd(db.name .. '/cols/' .. self.name .. '/ids', id)
    red:set(db.name .. '/cols/' .. self.name .. '/items/' .. id, serialize(new_doc))
    addIndexes(self, id, new_doc)
  end
end

function Collection:drop()
  assertIsInstance(self, Collection_mt, 'drop')
  local db, red = self.db, self.db.red
  red:srem(db.name .. '/cols', self.name)
  local script = ([[
    for _,k in ipairs(redis.call('keys', '%s/cols/%s*')) do
      redis.call('del', k)
    end
  ]]):format(db.name, self.name)
  db.collections[self.name] = nil
  return red:eval(script, 0)
end

function Collection:find(query, limit)
  assertIsInstance(self, Collection_mt, 'find')
  local db, red = self.db, self.db.red
  local items = {}
  local ids = findIdsMatchingQuery(self, query, limit)
  for i=1, #ids do
    local id = ids[i]
    local serialized = red:get(db.name .. '/cols/' .. self.name .. '/items/' .. id)
    items[i] = deserialize(serialized)
  end
  return items
end

function Collection:remove(query, justOne)
  assertIsInstance(self, Collection_mt, 'remove')
  local db, red = self.db, self.db.red
  local limit = justOne and 1 or math.huge
  local ids = findIdsMatchingQuery(self, query, limit)
  for i=1,#ids do
    local id = ids[i]
    removeIndexes(self, id)
    red:del(db.name .. '/cols/' .. self.name .. '/items/' .. id)
    red:srem(db.name .. '/cols/' .. self.name .. '/ids', id)
  end
  return len
end

----------------------------------------------------------

local Database = {}
local Database_mt = {
  name = 'Database',
  __index = function(self, key)
    if Database[key] then return Database[key] end
    return self:getCollection(key)
  end
}

function Database:dropDatabase()
  assertIsInstance(self, Database_mt, 'dropDatabase')
  local script = ([[
    for _,k in ipairs(redis.call('keys', '%s/*')) do
      redis.call('del', k)
    end
  ]]):format(self.name)
  return self.red:eval(script, 0)
end

function Database:getCollectionNames()
  assertIsInstance(self, Database_mt, 'getCollectionNames')
  local names = assert(self.red:smembers(self.name .. '/cols'))
  table.sort(names)
  return names
end

function Database:createCollection(collection_name)
  assertIsInstance(self, Database_mt, 'createCollection')
  local col = self:getCollection(collection_name)
  markAsExisting(col)
  return col
end

function Database:getCollection(collection_name)
  assertIsInstance(self, Database_mt, 'getCollection')
  return self.collections[collection_name] or setmetatable({
    db = self,
    name = collection_name
  }, Collection_mt)
end

----------------------------------------------------------

function modis.connect(red, options)
  options = options or {}
  return setmetatable({
    red         = red,
    name        = options.name or 'modis',
    collections = {}
  }, Database_mt)
end

return modis
