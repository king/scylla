package com.king.scylla.cache;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisCache extends Cache {
    private Jedis jedis;

    public RedisCache(JedisPool pool, int db) {
        jedis = pool.getResource();
        jedis.select(db);
    }

    public boolean exists(String key) throws CacheException {
        return jedis.exists(hash(key));
    }

    public void delete(String key) throws CacheException {
        jedis.del(hash(key));
        if (locked(key)) {
            unlock(key);
        }
    }

    public void expire(String key, int offset) throws CacheException {
        jedis.expire(hash(key), offset);
    }

    public JSONObject get(String key) throws CacheException {
        return new JSONObject(jedis.get(hash(key)));
    }

    public void set(String key, String val) throws CacheException {
        jedis.set(hash(key), val);
    }

    public boolean locked(String key) throws CacheException {
        return jedis.exists(lockname(key));
    }

    public void lock(String key) throws CacheException {
        jedis.set(lockname(key), "0");
    }

    public void unlock(String key) throws CacheException {
        jedis.del(lockname(key));
    }

    public void cleanup() throws CacheException {
        jedis.close();
    }

    private String lockname(String key) throws CacheException {
        return hash(key) + "-lock";
    }
}
