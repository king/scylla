// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import org.apache.commons.lang.NotImplementedException;

public enum Format {
    CSV("csv", "CSV"),
    JSON("json", "JSON");

    @SuppressWarnings("UnusedParameters")
    Format(String shortName, String name) {
    }

    public static Format fromString(String str) {
        switch (str.toLowerCase()) {
            case "csv":
                return CSV;
            case "json":
                return JSON;
            default:
                throw new NotImplementedException(String.format("Format '%s' not implemented", str));
        }
    }
}
