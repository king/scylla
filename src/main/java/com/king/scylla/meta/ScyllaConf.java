// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static com.king.scylla.meta.Scope.EXASOL;
import static com.king.scylla.meta.Scope.HIVE;
import static com.king.scylla.meta.Scope.REDSHIFT;
import static com.king.scylla.meta.Scope.IMPALA;


// TODO: add at least some of these parameters to ScyllaCLI
public class ScyllaConf {
    private Map<Scope, String> JDBCStrings = new HashMap<>();

    // default value, there needs to be one.
    private String cachePath = "/tmp/scylla.fcache";

    // cap for server-side caching, if you really need to store data
    private int cacheLifeTimeDays = 7;

    // use CSV serialisation (experimental)
    private Boolean csv;

    private Set<Scope> connectors = new HashSet<>();

    private static final Logger log = LogManager.getLogger(ScyllaConf.class.getName());

    private final String filename;

    public ScyllaConf check() throws ScyllaException {
        for(Scope scope : Scope.values()) {
            try {
                Class.forName(scope.getConnectorClass());
                connectors.add(scope);
                if(getDefaultJDBCStringForSope(scope) == null) {
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

    public void init(Properties properties) {
        setDefaultJDBCStringForScope(HIVE, properties.getProperty("hive_jdbcstring"));
        setDefaultJDBCStringForScope(EXASOL, properties.getProperty("exasol_jdbcstring"));
        setDefaultJDBCStringForScope(REDSHIFT, properties.getProperty("redshift_jdbcstring"));
        setDefaultJDBCStringForScope(IMPALA, properties.getProperty("impala_jdbcstring"));

        String csv = properties.getProperty("csv");
        if (csv != null) {
            csv = csv.toLowerCase().trim();
            setCsv(csv.equals("1") || csv.equals("true") || csv.equals("yes"));
        } else {
            setCsv(false);
        }

        if (properties.containsKey("cache_path")) {
            setCachePath(properties.getProperty("cache_path"));
        }

        // sets the cache lifetime. default value is 7.
        if (properties.containsKey("cache_lifetime_days")) {
            Integer cacheLifeTimeDays;

            try {
                cacheLifeTimeDays = Integer.parseInt(properties.getProperty("cache_lifetime_days"));
            } catch (NumberFormatException e) {
                cacheLifeTimeDays = null;
            }

            if (cacheLifeTimeDays == null || cacheLifeTimeDays <= 0) {
                log.warn("Parameter 'cache_lifetime_days' wasn't parsed correctly. Defaulting to 7 days.");
                cacheLifeTimeDays = 7;
            }

            setCacheLifeTimeDays(cacheLifeTimeDays);
        }
    }

    public ScyllaConf(String filename) {
        Properties props = new Properties();
        InputStream ifs = null;
        this.filename = filename;
        if(!(new File(filename).exists())) {
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
    ScyllaConf(Properties props) {
        this.filename = "";
        init(props);
    }

    public String getDefaultJDBCStringForSope(Scope scope) {
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

    public void setDefaultJDBCStringForScope(Scope scope, String string) {
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

    public void setCacheLifeTimeDays(int cacheLifeTimeDays) {
        this.cacheLifeTimeDays = cacheLifeTimeDays;
    }

    public String getCachePath() {
        return cachePath;
    }

    public void setCachePath(String cachePath) {
        this.cachePath = cachePath;
    }

    public String getFilename() {
        return filename;
    }

    public Boolean isCsv() {
        return csv;
    }

    public void setCsv(Boolean csv) {
        this.csv = csv;
    }
}
