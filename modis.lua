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

local OPERATORS = {}
for op in ("$gt $gte $lt $lte $in $not"):gmatch("%$%w+") do OPERATORS[op] = true end

local rep, concat, floor, max, min = string.rep, table.concat, math.floor, math.max, math.min

local function isEmpty(tbl)
  return next(tbl) == nil
end

local function isArray(obj)
  if type(obj) ~= 'table' then return false end
  local maximum, count = 0, 0
  for k, _ in pairs(obj) do
    if type(k) ~= 'number' or k < 0 or floor(k) ~= k then return false end
    maximum, count = max(maximum, k), count + 1
  end
  return count == maximum
end

local function isArrayOfPrimitives(obj)
  if not isArray(obj) then return false end
  for i = 1, #obj do
    local tvalue = type(obj[i])
    if tvalue ~= 'string' and tvalue ~= 'number' and tvalue ~= 'boolean' then
      return false
    end
  end
  return true
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

local function array_union(arr1, arr2)
  local values_set = {}
  for _,v in ipairs(arr1) do values_set[v] = true end
  for _,v in ipairs(arr2) do values_set[v] = true end
  local result, len = {}, 0
  for k,_ in pairs(values_set) do
    len = len + 1
    result[len] = k
  end
  return result
end

local function array_intersect(arr1, arr2)
  local values_set = {}
  for _,v in ipairs(arr1) do values_set[v] = true end
  local result, len = {}, 0
  for _,v in ipairs(arr2) do
    if values_set[v] then
      len = len + 1
      result[len] = v
    end
  end
  return result
end

local function array_difference(arr1, arr2)
  local values_set = {}
  for _,v in ipairs(arr2) do values_set[v] = true end
  local result, len = {}, 0
  for _,v in ipairs(arr1) do
    if not values_set[v] then
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
  if type(doc) ~= 'table' or hasOperators(doc) or isArrayOfPrimitives(doc) then
    result[prefix] = doc
  else
    for k,v in pairs(doc) do
      k = prefix and (prefix .. '.' .. tostring(k)) or k
      recursive_flatten(v, result, k)
    end
  end
  return result
end

-- flatten({a = 1, foo = { bar = 'baz' }}) = {a = 1, ['foo.bar'] = 'baz'}
local function flatten(doc)
  if type(doc) ~= 'table' then error('can not flatten non-tables: ' .. tostring(doc)) end
  if isEmpty(doc)         then return {} end
  if hasOperators(doc)    then error('the provided docect had operators (such as $lt) on its first-level keys') end
  return recursive_flatten(doc, {})
end

----------------------------------------------------------

local function getDocById(collection, id)
  return deserialize(collection.conn.red:get(collection.namespace .. '/docs/' .. id))
end

local function getIndexKey(collection, indexName, value)
  local red    = collection.conn.red
  local vtype  = type(value)
  local key    = {collection.namespace, '/index/', vtype, '?', indexName }
  if vtype ~= 'number' then
    local len = #key
    key[len+1], key[len+2] = '=', tostring(value)
  end
  return concat(key)
end

local function assertNumericOperatorType(indexName, operator, value)
  if value == nil then return end
  local vtype = type(value)
  if vtype ~= 'number' then error(indexName .. '.' .. operator .. ' must be a number. Was ' .. tostring(value) .. '(' .. vtype .. ')') end
end

local function findIdsMatchingIndex(collection, indexName, value)
  local red           = collection.conn.red
  local vtype         = type(value)
  local ids

  if vtype == 'string' or vtype == 'boolean' or vtype == 'number' then
    local key = getIndexKey(collection, indexName, value)
    if vtype == 'number' then
      ids = red:zrangebyscore(key, value, value)
    else --string or boolean
      ids = red:smembers(key)
    end
  elseif vtype == 'table' then
    if     isEmpty(value) then
      error('value in ' .. indexName .. ' can not be an empty table')
    elseif isArrayOfPrimitives(value) then
      ids = findIdsMatchingIndex(collection, indexName, {['$in'] = value})
    elseif hasOperators(value) then
      if value['$lt'] or value['$lte'] or value['$gt'] or value['$gte'] then
        local lt, lte, gt, gte = value['$lt'], value['$lte'], value['$gt'], value['$gte']

        assertNumericOperatorType(indexName, '$lt',  lt)
        assertNumericOperatorType(indexName, '$lte', lte)
        assertNumericOperatorType(indexName, '$gt',  gt)
        assertNumericOperatorType(indexName, '$gte', gte)

        local rmin = (gt and "(" .. tostring(gt)) or gte or "-inf"
        local rmax = (lt and "(" .. tostring(lt)) or lte or "+inf"

        local key = getIndexKey(collection, indexName, 0) -- Use 0 to get a numeric key
        ids = red:zrangebyscore(key, rmin, rmax)

      elseif value['$in'] then

        local value_in = value['$in']
        if not isArray(value_in) then error('expected array after $in operator in ' .. indexName) end
        ids = {}
        for i = 1, #value_in do
          ids = array_union(ids, findIdsMatchingIndex(collection, indexName, value_in[i]))
        end

      elseif value['$not'] then

        local not_ids = findIdsMatchingIndex(collection, indexName, value['$not'])
        local all_ids = red:smembers(collection.namespace .. '/docs')
        ids = array_difference(all_ids, not_ids)

      end
    else
      error('expected an array of primitives or a table with operators in ' .. indexName)
    end
  else
    error('unknown value type in ' .. indexName .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
  end

  return ids
end

local function findIdsMatchingQuery(collection, query)
  local red = collection.conn.red
  query = query or {}

  local ids = red:smembers(collection.namespace .. '/docs')

  for indexName,value in pairs(flatten(query)) do
    if isEmpty(ids) then break end
    local new_ids = findIdsMatchingIndex(collection, indexName, value)
    ids = array_intersect(ids, new_ids)
  end

  return ids
end

----------------------------------------------------------
local Cursor = {}
local Cursor_mt = {__index = Cursor, name = 'Cursor'}

function newCursor(collection, query)
  local cursor = setmetatable({
    collection = collection,
    conn       = collection.conn,
    query = query,
    _limit = math.huge
  }, Cursor_mt)

  cursor.ids = findIdsMatchingQuery(collection, query)

  return cursor
end

local function identity(obj) return obj end

function Cursor:toArray()
  assertIsInstance(self, Cursor_mt, 'toArray')
  return self:map(identity)
end

function Cursor:limit(limit)
  assertIsInstance(self, Cursor_mt, 'limit')
  self._limit = limit
  return self
end

function Cursor:map(f)
  assertIsInstance(self, Cursor_mt, 'map')
  local result, len = {},0
  self:forEach(function(doc)
    len = len + 1
    result[len] = f(doc)
  end)
  return result
end

function Cursor:forEach(f)
  assertIsInstance(self, Cursor_mt, 'forEach')
  local red = self.conn.red
  local ids = self.ids
  for i=1, self:count() do
    f(getDocById(self.collection, ids[i]))
  end
end

function Cursor:count()
  return math.min(#self.ids, self._limit)
end

----------------------------------------------------------
local Collection = {}
local Collection_mt = {__index = Collection, name = 'Collection'}

local function removeIndex(collection, id, indexName, value)
  local red    = collection.conn.red
  local vtype  = type(value)

  if vtype == 'string' or vtype == 'boolean' or vtype == 'number' then
    local key = getIndexKey(collection, indexName, value)
    if vtype == 'number' then
      red:zrem(key, id)
    else --string or boolean
      red:srem(key, id)
    end
  else
    error('unknown value type for object ' .. collection.name .. '/' .. tostring(id) .. '.' .. indexName  .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
  end
end

local function removeIndexes(collection, doc)
  local id = doc._id
  for indexName, value in pairs(flatten(doc)) do
    removeIndex(collection, id, indexName, value)
  end
end

local function addIndex(collection, id, indexName, value)
  local red   = collection.conn.red
  local vtype = type(value)

  if vtype == 'string' or vtype == 'boolean' or vtype == 'number' then
    local key = getIndexKey(collection, indexName, value)
    if vtype == 'number' then
      red:zadd(key, value, id)
    else -- string or boolean
      red:sadd(key, id)
    end
  else
    error('unknown value type for object ' .. collection.name .. '/' .. tostring(id) .. '.' .. indexName  .. ': ' .. tostring(value) .. '(' .. tvalue .. ')')
  end
end

local function addIndexes(collection, doc)
  local id = doc._id

  for indexName, value in pairs(flatten(doc)) do
    addIndex(collection, id, indexName, value, 0)
  end
end

function Collection:exists()
  assertIsInstance(self, Collection_mt, 'exists')
  return self.conn.red:sismember(self.db.namespace .. '/cols', self.name)
end

function Collection:count(query)
  assertIsInstance(self, Collection_mt, 'count')
  query = query or {}
  if isEmpty(query) then
    return self.conn.red:scard(self.namespace .. '/docs')
  else
    return newCursor(self, query):count()
  end
end

function Collection:insert(doc)
  assertIsInstance(self, Collection_mt, 'insert')

  local db, red = self.db, self.conn.red
  local docs = isArray(doc) and doc or {doc}

  db.conn:createDatabase(db.name)
  db:createCollection(self.name)

  for _,doc in ipairs(docs) do
    local new_doc = table_merge({}, doc)

    new_doc._id = new_doc._id or self:count() + 1 -- FIXME not thread safe
    local id = new_doc._id

    local is_new = red:sismember(self.namespace .. '/docs', id)
    if is_new then removeIndexes(red, self.namespace, new_doc) end

    red:sadd(self.namespace .. '/docs', id)
    red:set(self.namespace .. '/docs/' .. id, serialize(new_doc))
    addIndexes(self, new_doc)
  end
end

function Collection:drop()
  assertIsInstance(self, Collection_mt, 'drop')
  local db, red = self.db, self.conn.red
  red:srem(db.namespace .. '/cols', self.name)
  local script = ([[
    for _,k in ipairs(redis.call('keys', '%s*')) do
      redis.call('del', k)
    end
  ]]):format(self.namespace)
  db.collections[self.name] = nil
  return red:eval(script, 0)
end

function Collection:find(query)
  assertIsInstance(self, Collection_mt, 'find')
  return newCursor(self, query)
end

function Collection:findOne(query)
  assertIsInstance(self, Collection_mt, 'findOne')
  return self:find(query):limit(1):toArray()[1]
end

function Collection:remove(query, justOne)
  assertIsInstance(self, Collection_mt, 'remove')
  local red       = self.conn.red
  local namespace = self.namespace
  local cursor    = newCursor(self, query)
  if justOne then cursor:limit(1) end

  cursor:forEach(function(doc)
    removeIndexes(self, doc)
    red:del(namespace .. '/docs/' .. doc._id)
    red:srem(namespace .. '/docs', doc._id)
  end)
end

function Collection:update(query, modifications)
  assertIsInstance(self, Collection_mt, 'update')
  local red = self.conn.red
  newCursor(self, query):forEach(function(doc)
    local key = self.namespace .. '/docs/' .. doc._id
    local doc = getDocById(self, doc._id)
    removeIndexes(self, doc)

    table_merge(doc, modifications)
    red:set(key, serialize(doc))
    addIndexes(self, doc)
  end)
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
  local red = self.conn.red

  red:srem(self.conn.namespace .. '/dbs', self.name)
  local script = ([[
    for _,k in ipairs(redis.call('keys', '%s*')) do
      redis.call('del', k)
    end
  ]]):format(self.namespace)
  red:eval(script, 0)
end

function Database:getCollectionNames()
  assertIsInstance(self, Database_mt, 'getCollectionNames')
  local names = assert(self.conn.red:smembers(self.namespace .. '/cols'))
  table.sort(names)
  return names
end

function Database:createCollection(collection_name)
  assertIsInstance(self, Database_mt, 'createCollection')
  local col = self:getCollection(collection_name)
  self.collections[collection_name] = col
  assert(self.conn.red:sadd(self.namespace .. '/cols', collection_name))
  return col
end

function Database:getCollection(collection_name)
  assertIsInstance(self, Database_mt, 'getCollection')
  return self.collections[collection_name] or setmetatable({
    db = self,
    conn = self.conn,
    name = collection_name,
    namespace = self.namespace .. '/cols/' .. collection_name
  }, Collection_mt)
end

function Database:exists()
  assertIsInstance(self, Database_mt, 'exists')
  return self.conn.red:sismember(self.conn.namespace .. '/dbs', self.name)
end

----------------------------------------------------------

local Connection = {}
local Connection_mt = {__index = Connection, name = 'Connection'}

function Connection:getDatabase(databaseName)
  assertIsInstance(self, Connection_mt, 'getDatabase')
  assert(type(databaseName) == 'string', 'Database name required')

  return self.databases[databaseName] or setmetatable({
    conn = self,
    name = databaseName,
    namespace = self.namespace .. '/dbs/' .. databaseName,
    collections = {}
  }, Database_mt)
end

function Connection:createDatabase(databaseName)
  self.databases[databaseName] = self:getDatabase(databaseName)
  self.red:sadd(self.namespace .. '/dbs', databaseName)
  return self.databases[databaseName]
end

function Connection:getDatabaseNames()
  assertIsInstance(self, Connection_mt, 'getDatabaseNames')
  local names = self.red:smembers(self.namespace .. '/dbs')
  table.sort(names)
  return names
end

function Connection:shutdown()
  assertIsInstance(self, Connection_mt, 'shutdown')
  self.red = nil
end

----------------------------------------------------------

function modis.connect(red)
  return setmetatable({
    red         = red,
    databases   = {},
    namespace   = '/modis'
  }, Connection_mt)
end

return modis
