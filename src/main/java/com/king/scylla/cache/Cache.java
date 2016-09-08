// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.cache;

import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class Cache {
    abstract public boolean exists(String key) throws CacheException;

    abstract public void delete(String key) throws CacheException;

    abstract public void expire(String key, int offset) throws CacheException;

    abstract public JSONObject get(String key) throws CacheException;

    abstract public void set(String key, String val) throws CacheException;

    abstract public boolean locked(String key) throws CacheException;

    abstract public void lock(String key) throws CacheException;

    abstract public void unlock(String key) throws CacheException;

    static String hash(String text) throws CacheException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes(StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            StringBuilder hs = new StringBuilder();

            for (byte aHash : hash) {
                if ((0xff & aHash) < 0x10) {
                    hs.append("0").append(Integer.toHexString((0xFF & aHash)));
                } else {
                    hs.append(Integer.toHexString(0xFF & aHash));
                }
            }

            return hs.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }
}
