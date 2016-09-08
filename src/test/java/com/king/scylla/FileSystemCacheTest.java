// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import com.king.scylla.cache.FileSystemCache;
import com.king.scylla.cache.CacheException;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSystemCacheTest {
    FileSystemCache fc;
    String path = "/tmp/test.scylla.fcache." + Math.abs(new Random().nextLong()) + "." + new Date().getTime();

    @Before
    public void init() throws CacheException {
        fc = new FileSystemCache(path);
    }

    private String getRandomKey() {
        return String.format("junit_test_%d_%d", new DateTime().getMillis(), new Random().nextLong());
    }

    private void createElement(String k) throws IOException, CacheException {
        fc.set(k, "test");
    }

    private void cleanElement(String k) throws IOException, CacheException {
        fc.delete(k);
    }

    @Test
    public void basicFunctionalityTest() throws IOException, CacheException {
        String k = getRandomKey();

        createElement(k);
        assertTrue(fc.exists(k));

        cleanElement(k);
        assertFalse(fc.exists(k));
    }

    @Test
    public void lockTest() throws IOException, CacheException {
        String k = getRandomKey();

        createElement(k);

        fc.lock(k);
        assertTrue(fc.locked(k));

        fc.unlock(k);
        assertFalse(fc.locked(k));

        cleanElement(k);
    }

    @After
    public void cleanup() throws IOException {
        Path p = Paths.get(path);
        Files.walk(p).map(Path::toFile).forEach(File::delete);
        Files.delete(p);
    }
}
