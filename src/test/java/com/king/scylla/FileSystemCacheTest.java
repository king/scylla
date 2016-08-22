// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSystemCacheTest {
    FileSystemCache fc = new FileSystemCache("/tmp/scylla.fcache");

    public FileSystemCacheTest() throws FileSystemCacheException {
    }

    private String getRandomKey() {
        return String.format("junit_test_%d_%d", new DateTime().getMillis(), new Random().nextLong());
    }

    private void createElement(String k) throws IOException, FileSystemCacheException {
        fc.set(k, "test");
    }

    private void cleanElement(String k) throws IOException, FileSystemCacheException {
        fc.delete(k);
    }

    @Test
    public void basicFunctionalityTest() throws IOException, FileSystemCacheException {
        String k = getRandomKey();

        createElement(k);
        assertTrue(fc.exists(k));

        cleanElement(k);
        assertFalse(fc.exists(k));
    }

    @Test
    public void lockTest() throws IOException, FileSystemCacheException {
        String k = getRandomKey();

        createElement(k);

        fc.lock(k);
        assertTrue(fc.locked(k));

        fc.unlock(k);
        assertFalse(fc.locked(k));

        cleanElement(k);
    }
}
