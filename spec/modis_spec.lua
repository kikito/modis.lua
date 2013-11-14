local modis = require 'modis'
local redis = require 'redis'

describe('modis', function()
  local red, conn, db
  before_each(function()
    red  = redis.connect('127.0.0.1', 6379)
    conn = modis.connect(red)
    db   = conn:getDatabase('test')
  end)
  after_each(function()
    pcall(function()
      db:dropDatabase()
      red:quit()
    end)
  end)

  describe('Connection', function()
    describe(':getDatabase', function()
      it('returns a database', function()
        local db2 = conn:getDatabase('foo')
        assert.is_truthy(db2)
      end)
      it('caches the database', function()
        local db2 = conn:createDatabase('foo')
        local db3 = conn:getDatabase('foo')
        assert.equals(db3, db2)
      end)
    end)
    describe(':getDatabaseNames', function()
      it('returns the names of all the databases', function()
        conn:createDatabase('foo')
        conn:createDatabase('bar')
        assert.same(conn:getDatabaseNames(), {'bar', 'foo'})
      end)
    end)
    describe(':shutdown', function()
      it('throws an error if you try to use redis after', function()
        conn:shutdown()
        assert.error(function() db.users:add({name = 'peter'}) end)
      end)
    end)
  end)

  describe('Database', function()
    describe(':getCollectionNames', function()
      it('is initially empty', function()
        assert.same({}, db:getCollectionNames())
      end)
      it('returns the created collections', function()
        db:createCollection('users')
        db:createCollection('projects')
        assert.same({'projects', 'users'}, db:getCollectionNames())
      end)
    end)

    describe(':getCollection', function()
      describe('when the collection does not exist', function()
        it('does not return the same reference when called twice', function()
          assert.not_equal(db:getCollection('users'), db:getCollection('users'))
        end)
      end)
      describe('when the collection exists', function()
        it('returns the same collection', function()
          db:createCollection('users')
          assert.equal(db:getCollection('users'), db:getCollection('users'))
        end)
        it('does not return the same collection if the collection is removed', function()
          db:createCollection('users')
          db:getCollection('users'):drop()
          assert.not_equal(db:getCollection('users'), db:getCollection('users'))
        end)
      end)
    end)

    describe('.<collectionname>', function()
      describe('when the collection does not exist', function()
        it('does not return the same reference when called twice', function()
          assert.not_equal(db.users, db.users)
        end)
      end)
    end)

    describe('when there is another database with the same name', function()
      local db2
      before_each(function()
        db2 = conn:getDatabase('test')
      end)
      it('can have items added via the other db', function()
        db2.users:insert({name = 'peter'})
        assert.equal(1, db.users:count())
      end)
      it('can have collections created by the other db', function()
        db2:createCollection('users')
        assert.is_true(db.users:exists())
      end)
      it('can be dropped by the other db', function()
        db:createCollection('users')
        db2:dropDatabase()
        assert.is_false(db.users:exists())
      end)
    end)
  end) -- DB

  describe('Collection', function()

    describe(':exists', function()
      describe('when nothing has been done on the table', function()
        it('returns false', function()
          assert.is_false(db.users:exists())
        end)
      end)
      describe('when the table has been created with createCollection', function()
        it('returns true', function()
          db:createCollection('users')
          assert.is_true(db.users:exists())
        end)
      end)
    end)

    describe(':count', function()
      it('starts at 0', function()
        assert.equals(0, db.users:count())
      end)
      it('can count with a query', function()
        db.users:insert({{a=1}, {a=2}, {a=3}})
        assert.equals(2, db.users:count({a = {['$lt'] = 3}}))
      end)
    end)

    describe(':insert', function()
      it('checks self', function()
        assert.error(function() db.users.insert({dummy = true}) end)
      end)
      it('marks the table as existing', function()
        db.users:insert({})
        assert.is_true(db.users:exists())
      end)
      it('marks the database as existing', function()
        assert.is_false(db:exists())
        db.users:insert({})
        assert.is_true(db:exists())
      end)
      it('increases the count', function()
        for i=1,3 do db.users:insert({dummy = 'dummy'}) end
        assert.equals(3, db.users:count())
      end)
      it('adds an _id field if the object does not have it', function()
        db.users:insert({name = 'laurent'})
        assert.same(db.users:find({_id = 1}):toArray(), {{name = 'laurent', _id = 1}})
        db.users:insert({name = 'harvey', _id = 3})
        assert.same(db.users:find({_id = 3}):toArray(), {{name = 'harvey', _id = 3}})
      end)
      it('can insert in bulk', function()
        db.users:insert({ {name = 'athos'}, {name = 'aramis'}, {name = 'porthos'} })
        assert.equals(3, db.users:count())
      end)
    end)

    describe(':find', function()
      it('returns all elements when the query is empty', function()
        for i=1,3 do db.users:insert({foo = 'foo'}) end
        assert.equals(#db.users:find({}):toArray(), 3)
      end)
      it('returns an item given its id', function()
        db.users:insert({name = 'joey',     _id = 1})
        db.users:insert({name = 'chendler', _id = 2})
        db.users:insert({name = 'ross',     _id = 3})
        assert.same({{name = 'joey',     _id = 1}}, db.users:find({_id = 1}):toArray())
        assert.same({{name = 'chendler', _id = 2}}, db.users:find({_id = 2}):toArray())
        assert.same({{name = 'ross',     _id = 3}}, db.users:find({_id = 3}):toArray())
      end)
      it('respects the limit param', function()
        db.users:insert({{a=1}, {a=1}, {a=1}})
        assert.same({{a=1,_id=1}, {a=1,_id=2}}, db.users:find({}):limit(2):toArray())
        assert.same({}, db.users:find({}):limit(0):toArray())
      end)
      it('can find based on string params', function()
        db.users:insert({{name='albert'}, {name='fred'}})
        assert.same({{name='albert', _id = 1}}, db.users:find({name = 'albert'}):toArray())
      end)
      it('can find based on boolean params', function()
        db.users:insert({{name='albert', active=true}, {name='fred', active=false}})
        assert.same({{name='albert', _id = 1, active=true}}, db.users:find({active = true}):toArray())
      end)
      it('can find based on number params', function()
        db.users:insert({{name='albert', age=18}, {name='fred', age=16}})
        assert.same({{name='albert', _id = 1, age=18}}, db.users:find({age=18}):toArray())
      end)
      it('can find based on an array of values', function()
        db.users:insert({{name='albert'}, {name='fred'}, {name='peter'}})
        assert.same({{name='albert', _id = 1}, {name='peter', _id=3}}, db.users:find({name = {'albert', 'peter'}}):toArray())
      end)
      it('understands $in', function()
        db.users:insert({{name='albert'}, {name='fred'}, {name='peter'}})
        assert.same({{name='albert', _id = 1}, {name='peter', _id=3}}, db.users:find({name = {['$in'] = {'albert', 'peter'}}}):toArray())
      end)
      it('understands $gt', function()
        db.users:insert({{name = 'billy', age=15}, {name='jimmy', age=18}})
        assert.same({{name='jimmy', age=18, _id=2}}, db.users:find({age = {['$gt'] = 16}}):toArray())
      end)
      it('understands $not', function()
        db.users:insert({{name = 'billy', age=15}, {name='jimmy', age=18}})
        assert.same({{name='jimmy', age=18, _id=2}}, db.users:find({age = {['$not'] = {['$lt'] = 18}}}):toArray())
      end)
    end)

    describe(':findOne', function()
      it('returns nil when a collection is empty', function()
        assert.is_nil(db.users:findOne({}))
      end)
      it('returns nil when no element matches', function()
        db.evangels:insert({{author='matthew'}, {author='marcus'}, {author='lucas'}, {author='john'}})
        assert.is_nil(db.evangels:findOne({author='rufus'}))
      end)
      it('returns the first element available, even when there are more than 1', function()
        db.users:insert({{name = 'billy', age=15}, {name='jimmy', age=18}})
        assert.same({name='billy', age=15, _id=1}, db.users:findOne({age = {['$lt'] = 16}}))
      end)
    end)

    describe(':update', function()
      it('can change existing fields of 1 doc with a basic query', function()
        db.users:insert({{name = 'billy'}, {name='jimmy'}})
        db.users:update({name = 'billy'}, {name = 'karnov'})
        assert.same({{name='karnov', _id=1}, {name='jimmy', _id=2}}, db.users:find():toArray())
      end)
      it('can update several docs', function()
        db.heroes:insert({{name='batman'}, {name='superman'}, {name='spiderman'}})
        db.heroes:update({name = {['$not'] = 'spiderman'}}, {publisher = 'dc'})
        assert.same(db.heroes:findOne({name='batman'}).publisher, 'dc')
        assert.same(db.heroes:findOne({name='superman'}).publisher, 'dc')
        assert.is_nil(db.heroes:findOne({name='spiderman'}).publisher)
      end)
      it('can add new fields', function()
        db.gods:insert({name = 'kali'})
        db.gods:update({name = 'kali'}, {arms=12})
        assert.same({{name='kali', _id=1, arms=12}}, db.gods:find():toArray())
      end)
      it('can update all elements', function()
        db.sushi:insert({{futomaki = true}, {nori = true}})
        db.sushi:update({}, {rice = true})
        assert.same({{futomaki = true, rice = true, _id = 1},
                     {nori = true, rice = true, _id = 2}},
                     db.sushi:find():toArray())
      end)
      -- missing: operator $unset for removing fields
    end)

    describe(':drop', function()
      it('removes all the elements from the collection', function()
        db.users:insert({{a=1}, {a=2}, {a=3}})
        assert.equal(db.users:count(), 3)
        db.users:drop()
        assert.equal(db.users:count(), 0)
      end)
      it('marks the collection as non-existing', function()
        db:createCollection('users')
        assert.is_true(db.users:exists())
      end)
    end)

    describe(':remove', function()
      it('removes all documents when given no params', function()
        db.users:insert({{a=1}, {a=2}, {a=3}})
        db.users:remove()
        assert.equal(db.users:count(), 0)
      end)
      it('removes the document with the given id', function()
        db.users:insert({{a=1}, {a=2}, {a=3}})
        db.users:remove({_id = 2})
        assert.same(db.users:find({}):toArray(), {{a=1, _id=1}, {a=3, _id=3}})
      end)
      it('respects the justOne', function()
        db.users:insert({{a=1}, {a=1}, {a=1}})
        db.users:remove({}, true)
        assert.equals(2, db.users:count())
        db.users:remove({}, false)
        assert.equals(0, db.users:count())
      end)
      it('does not remove anything if the request does not match anything', function()
        db.users:insert({name = 'randy'})
        db.users:remove({name = 'donald'})
        assert.same({{name = 'randy', _id = 1}}, db.users:find({}):toArray())
      end)
      it('can delete 1 doc with a basic query', function()
        db.users:insert({{name = 'billy'}, {name='jimmy'}})
        db.users:remove({name = 'jimmy'})
        assert.same({{name='billy', _id=1}}, db.users:find():toArray())
      end)
      it('can delete several docs', function()
        db.heroes:insert({{name='batman'}, {name='superman'}, {name='spiderman'}})
        db.heroes:remove({name = 'spiderman'})
        assert.same({{name='batman', _id=1}, {name='superman', _id=2}}, db.heroes:find():toArray())
      end)
      it('can delete all docs', function()
        db.sushi:insert({{futomaki = true}, {nori = true}})
        db.sushi:remove({})
        assert.same({}, db.sushi:find():toArray())
      end)
    end)
  end)

  describe('Cursor', function()
    describe(':toArray()', function()
      it('returns an empty table when there are no elements', function()
        assert.same(db.people:find():toArray(), {})
      end)
    end)
  end)
end)

