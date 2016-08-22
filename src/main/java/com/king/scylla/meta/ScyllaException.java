// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

public class ScyllaException extends Exception {
    public ScyllaException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScyllaException(String message) {
        super(message);
    }
}
