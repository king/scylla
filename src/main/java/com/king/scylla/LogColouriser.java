// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import org.apache.commons.io.Charsets;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/*
    There's probably a better way to do this. This object is created once per connection and assigns a colour to
    each query (deterministically) so that users can remember which one they're running by looking at the log.
 */
public class LogColouriser {
    // the pairs in this array are (fg, bg) pairs that will be combined with BASH escape sequences to be turned
    // into colours (e.g. "033[32m033[43m033[1m")
    private static int[][] colours = {{7, 2}, {7, 3}, {7, 4}, {7, 5}, {7, 6}, {7, 7}, {0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 7}};

    private int x;
    private int y;

    LogColouriser() {
        int i = (new Random()).nextInt(colours.length - 1);
        init(i);
    }

    LogColouriser(String query) {
        init(query);
    }

    static String whiteOnBlack(String text) {
        return String.format("\033[40m\033[37m\033[1m%s\033[0m", text);
    }

    static String whiteOnPink(String text) {
        return String.format("\033[41m\033[37m\033[1m%s\033[0m", text);
    }

    String colorise(String text) {
        return String.format("\033[%dm\033[%dm\033[1m%s\033[0m", this.x, this.y, text);
    }

    public String cuteLog(String name, String text) {
        return String.format("[%s] %s", colorise(name), text);
    }

    public String toString() {
        return String.format("LogColouriser(tm): (%d, %d)", x, y);
    }

    private void init(int seed) {
        int[] a = colours[seed];
        x = 30 + a[0];
        y = 40 + a[1];
    }

    public void init(String query) {
        BigInteger a;
        try {
            a = new BigInteger(MessageDigest.getInstance("MD5").digest(query.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            a = new BigInteger(String.valueOf(new Random().nextInt()));
        }
        BigInteger b = new BigInteger(String.valueOf(colours.length));
        int i = a.mod(b).intValue();
        init(i);
    }
}
