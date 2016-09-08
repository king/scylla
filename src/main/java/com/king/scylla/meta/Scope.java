// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import org.apache.commons.lang.NotImplementedException;

public enum Scope {
    HIVE("hive", "Hive", false, "org.apache.hive.jdbc.HiveDriver"),
    EXASOL("exasol", "Exasol", true, "com.exasol.jdbc.EXADriver"),
    REDSHIFT("redshift", "Redshift", true, "com.amazon.redshift.jdbc41.Driver"),
    IMPALA("impala", "Impala", false, "com.cloudera.impala.jdbc41.Driver");

    private String name;
    private boolean needsPassword = false;
    private String connectorClass;

    @SuppressWarnings("UnusedParameters")
    Scope(String shortName, String name, boolean needsPassword, String connectorClass) {
        this.name = name;
        this.needsPassword = needsPassword;
        this.connectorClass = connectorClass;
    }

    public static Scope getScopeFromShortName(String shortName) {
        switch (shortName.toLowerCase()) {
            case "exa":
            case "exasol":
                return EXASOL;
            case "hive":
                return HIVE;
            case "redshift":
                return REDSHIFT;
            case "impala":
                return IMPALA;
            default:
                throw new NotImplementedException(String.format("Scope '%s' doesn't exist.", shortName));
        }
    }

    public String toString() {
        return getName();
    }

    public boolean needsPassword() {
        return this.needsPassword;
    }

    public String getName() {
        return name;
    }

    public String getConnectorClass() {
        return connectorClass;
    }

    public String classNotFound() {
        return String.format("%s connector isn't configured: I couldn't find '%s'. Make sure that the " +
                        "%s driver is installed in the classpath or in /usr/share/java/scylla.",
                this.getName(), this.getConnectorClass(), this.getName());
    }
}
