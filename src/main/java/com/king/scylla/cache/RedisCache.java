package com.king.scylla.cache;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisCache extends Cache {
    private final JedisPool pool;
    private final int db;

    public RedisCache(JedisPool pool, int db) {
        this.pool = pool;
        this.db = db;
    }

    private Jedis jedis() {
        Jedis jedis = pool.getResource();
        jedis.select(db);
        return jedis;
    }

    public boolean exists(String key) throws CacheException {
        Jedis jedis = jedis();
        boolean e = jedis.exists(hash(key));
        jedis.close();
        return e;
    }

    public void delete(String key) throws CacheException {
        Jedis jedis = jedis();
        jedis.del(hash(key));
        if (locked(key)) {
            unlock(key);
        }
        jedis.close();
    }

    public void expire(String key, int offset) throws CacheException {
        Jedis jedis = jedis();
        jedis.expire(hash(key), offset);
        jedis.close();
    }

    public JSONObject get(String key) throws CacheException {
        Jedis jedis = jedis();
        JSONObject o = new JSONObject(jedis.get(hash(key)));
        jedis.close();
        return o;
    }

    public void set(String key, String val) throws CacheException {
        Jedis jedis = jedis();
        jedis.set(hash(key), val);
        jedis.close();
    }

    public boolean locked(String key) throws CacheException {
        Jedis jedis = jedis();
        boolean l = jedis.exists(lockname(key));
        jedis.close();
        return l;
    }

    public void lock(String key) throws CacheException {
        Jedis jedis = jedis();
        jedis.set(lockname(key), "0");
        jedis.close();
    }

    public void unlock(String key) throws CacheException {
        Jedis jedis = jedis();
        jedis.del(lockname(key));
        jedis.close();
    }

    private String lockname(String key) throws CacheException {
        return hash(key) + "-lock";
    }
}
