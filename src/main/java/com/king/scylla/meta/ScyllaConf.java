// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import com.king.scylla.cache.Cache;
import com.king.scylla.cache.CacheException;
import com.king.scylla.cache.FileSystemCache;
import com.king.scylla.cache.RedisCache;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

import static com.king.scylla.meta.Scope.EXASOL;
import static com.king.scylla.meta.Scope.HIVE;
import static com.king.scylla.meta.Scope.REDSHIFT;
import static com.king.scylla.meta.Scope.IMPALA;


public class ScyllaConf {
    private Map<Scope, String> JDBCStrings = new HashMap<>();

    // default value, there needs to be one.
    private String cachePath = "/tmp/scylla.fcache";

    // cap for server-side caching, if you really need to store data
    private int cacheLifeTimeDays = 7;

    private Format format = null;

    private Set<Scope> connectors = new HashSet<>();

    private static final Logger log = LogManager.getLogger(ScyllaConf.class.getName());

    private boolean redis = false;

    private int redisDB = 7;
    private String redisHost = "localhost";
    private JedisPool pool;

    public ScyllaConf check() throws ScyllaException {
        for (Scope scope : Scope.values()) {
            try {
                Class.forName(scope.getConnectorClass());
                connectors.add(scope);
                if (getDefaultJDBCStringForSope(scope) == null) {
                    log.warn(String.format("%s should work, but there is no default JDBC string for it. You should " +
                            "explicitly specify it in your queries.", scope.getName()));
                }
            } catch (ClassNotFoundException e) {
                log.warn(scope.classNotFound());
            }
        }

        if (connectors.size() == Scope.values().length) {
            log.debug("All connectors configured. Awesome");
        } else if (connectors.size() == 0) {
            throw new ScyllaException("No connectors configured, edit /etc/scylla.properties");
        } else {
            String s = connectors.stream().map(Object::toString).collect(Collectors.joining(", "));
            log.info("The following connectors are currently configured: " + s);
        }

        return this;
    }

    private int parseUInt(String prop) {
        try {
            return Integer.parseInt(prop);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void init(Properties properties) {
        setDefaultJDBCStringForScope(HIVE, properties.getProperty("hive_jdbcstring"));
        setDefaultJDBCStringForScope(EXASOL, properties.getProperty("exasol_jdbcstring"));
        setDefaultJDBCStringForScope(REDSHIFT, properties.getProperty("redshift_jdbcstring"));
        setDefaultJDBCStringForScope(IMPALA, properties.getProperty("impala_jdbcstring"));

        setRedis(yes(properties.getProperty("redis")));

        if (properties.containsKey("format")) {
            setFormat(Format.fromString(properties.getProperty("format")));
        }

        if (properties.containsKey("cache_path")) {
            setCachePath(properties.getProperty("cache_path"));
        }

        if (properties.containsKey("redis_host")) {
            setRedisHost(properties.getProperty("redis_host"));
        }


        if (properties.containsKey("redis_db")) {
            int redisDB = parseUInt(properties.getProperty("redis_db"));

            if (redisDB < 0) {
                log.warn("Parameter 'redis_db' wasn't parsed correctly. Defaulting to 666.");
            }
            setRedisDB(redisDB >= 0 ? redisDB : this.redisDB);
        }

        if (redis) {
            pool = new JedisPool(new JedisPoolConfig(), redisHost);
        }

        // sets the cache lifetime. default value is 7.
        if (properties.containsKey("cache_lifetime_days")) {
            int cacheLifeTimeDays = parseUInt(properties.getProperty("cache_lifetime_days"));

            if (cacheLifeTimeDays <= 0) {
                log.warn("Parameter 'cache_lifetime_days' wasn't parsed correctly. Defaulting to 7 days.");
            }
            setCacheLifeTimeDays(cacheLifeTimeDays <= 0 ? 7 : cacheLifeTimeDays);
        }
    }

    public ScyllaConf(String filename) {
        Properties props = new Properties();
        InputStream ifs = null;
        if (!(new File(filename).exists())) {
            log.warn("`/etc/scylla.properties` not found. You'll need to specify JDBC strings at runtime manually.");
            return;
        }

        try {
            ifs = new FileInputStream(filename);

            props.load(ifs);
            this.init(props);
        } catch (IOException e) {
            log.fatal("Error reading the configuration file. Exiting.", e);
            System.exit(-1);
        } finally {
            if (ifs != null) {
                try {
                    ifs.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }

    }

    // used only in tests.
    public ScyllaConf(Properties props) {
        init(props);
    }

    String getDefaultJDBCStringForSope(Scope scope) {
        switch (scope) {
            case HIVE:
                return JDBCStrings.getOrDefault(HIVE, null);
            case EXASOL:
                return JDBCStrings.getOrDefault(EXASOL, null);
            case IMPALA:
                return JDBCStrings.getOrDefault(IMPALA, null);
            case REDSHIFT:
                return JDBCStrings.getOrDefault(REDSHIFT, null);
            default:
                throw new NotImplementedException(String.format("Scope %s not implemented", scope));
        }
    }

    private void setDefaultJDBCStringForScope(Scope scope, String string) {
        JDBCStrings.put(scope, string);
    }

    public boolean isSupported(Scope scope) {
        return connectors.contains(scope);
    }

    public boolean supportsHive() {
        return connectors.contains(HIVE);
    }

    public int getCacheLifeTimeDays() {
        return cacheLifeTimeDays;
    }

    private void setCacheLifeTimeDays(int cacheLifeTimeDays) {
        this.cacheLifeTimeDays = cacheLifeTimeDays;
    }

    private String getCachePath() {
        return cachePath;
    }

    private void setCachePath(String cachePath) {
        this.cachePath = cachePath;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
        log.debug(String.format("Using %s as serialisation format", format));
    }

    private void setRedis(boolean redis) {
        if (redis) {
            log.info("Using Redis to cache. Query responsibly.");
        }
        this.redis = redis;
    }

    private void setRedisDB(int redisDB) {
        this.redisDB = redisDB;
    }

    private void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public Cache cache() throws CacheException {
        if (redis) {
            return new RedisCache(pool, redisDB);
        } else {
            return new FileSystemCache(getCachePath());
        }
    }

    private boolean yes(String prop) {
        if (prop != null) {
            prop = prop.toLowerCase().trim();
            return prop.equals("1") || prop.equals("true") || prop.equals("yes");
        } else {
            return false;
        }
    }
}
