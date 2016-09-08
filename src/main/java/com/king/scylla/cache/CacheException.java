// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.cache;

public class CacheException extends Exception {
    CacheException(String message, Throwable cause) {
        super(message, cause);
    }

    CacheException(String message) {
        super(message);
    }
}
