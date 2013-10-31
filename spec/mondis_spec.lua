local mondis = require 'mondis'
local redis = require 'redis'

describe('mondis', function()

  describe('db', function()
    local db, red
    before_each(function()
      red = redis.connect('127.0.0.1', 6379)
      db  = mondis.connect(red)
    end)
    after_each(function()
      db:dropDatabase()
      red:quit()
    end)

    describe(':getCollectionNames', function()
      it('is initially empty', function()
        assert.same({}, db:getCollectionNames())
      end)
      it('focus returns the created collections', function()
        db:createCollection('users')
        db:createCollection('projects')
        assert.same({'projects', 'users'}, db:getCollectionNames())
      end)
    end)

  end)


end)

