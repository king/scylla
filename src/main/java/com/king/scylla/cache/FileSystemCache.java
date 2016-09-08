// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.cache;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

// a generalisation of this thing could be quite useful for normal people to use.
public class FileSystemCache extends Cache {
    private final Path mpath;

    public FileSystemCache(String cachePath) throws CacheException {
        this.mpath = Paths.get(cachePath);
        if (!Files.exists(mpath)) {
            try {
                Files.createDirectory(mpath);
            } catch (IOException e) {
                throw new CacheException("Error creating the directory for the cache entry", e);
            }
            setPerms(mpath);
        }
        if (!Files.exists(mpath.resolve("data"))) {
            try {
                Files.createDirectory(mpath.resolve("data"));
            } catch (IOException e) {
                throw new CacheException("Error creating the directory for the cache entry", e);
            }
            setPerms(mpath.resolve("data"));
        }
        cleanup();
    }

    public boolean exists(String key) throws CacheException {
        return Files.exists(resolveKey(key));
    }

    public void delete(String key) throws CacheException {
        if (Files.exists(resolveKey(key))) {
            try {
                FileUtils.deleteDirectory(resolveKey(key).toFile());
            } catch(IOException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
    }

    public void expire(String key, int offset) throws CacheException {
        if (exists(key)) {
            Path expp = resolveKey(key).resolve("expire");
            try {
                if (Files.exists(expp)) {
                    Files.delete(expp);
                }
                Files.write(expp, ("" + (System.currentTimeMillis() / 1000 + offset)).getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new CacheException("Error while setting the new expiration time", e);
            }
            setPerms(expp);
        }
    }

    public JSONObject get(String key) throws CacheException {
        if (exists(key)) {
            Path loc = resolveKey(key);
            FileInputStream fin;
            try {
                fin = new FileInputStream(loc.resolve("data").toString());
            } catch (FileNotFoundException e) {
                throw new CacheException("File '" + loc.resolve("data").toString() + "' not found!", e);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(fin));
            try {
                JSONObject jo = new JSONObject(br.readLine());
                br.close();
                fin.close();

                return jo;
            } catch (IOException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
        return null;
    }

    public void set(String key, String val) throws CacheException {
        if (exists(key) && !locked(key)) {
            delete(key);
        } else if (locked(key)) {
            throw new CacheException("You are trying to overwrite a locked key");
        }
        Path loc = resolveKey(key);

        try {
            Files.createDirectory(loc);

            lock(key);
            Files.write(loc.resolve("key"), key.getBytes(StandardCharsets.UTF_8));
            setPerms(loc.resolve("key"));

            FileOutputStream fout = new FileOutputStream(loc.resolve("data").toString());
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout));
            bw.write(val);
            bw.close();
            fout.close();
            setPerms(loc.resolve("data"));
            unlock(key);
        } catch(IOException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    public boolean locked(String key) throws CacheException {
        return Files.exists(resolveKey(key).resolve("lock"));
    }

    public void lock(String key) throws CacheException {
        if (exists(key) && !locked(key)) {
            try {
                Files.write(resolveKey(key).resolve("lock"), "locked!".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new CacheException(e.getMessage(), e);
            }
            setPerms(resolveKey(key).resolve("lock"));
        }
    }

    public void unlock(String key) throws CacheException {
        if (exists(key) && locked(key)) {
            try {
                Files.delete(resolveKey(key).resolve("lock"));
            } catch (IOException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
    }

    public void cleanup() throws CacheException {
        String[] l = mpath.resolve("data").toFile().list();
        if (l != null) {
            for (String dir : l) {
                Path loc = mpath.resolve("data").resolve(dir);
                if (Files.exists(loc.resolve("expire"))) {
                    try {
                        Long expire = Long.valueOf(Files.readAllLines(loc.resolve("expire"), StandardCharsets.UTF_8).get(0));
                        if (expire <= System.currentTimeMillis() / 1000) {
                            FileUtils.deleteDirectory(loc.toFile());
                        }
                    } catch (IOException e) {
                        throw new CacheException(e.getMessage(), e);
                    }
                }
            }
        }
    }

    private void setPerms(Path path) throws CacheException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        try {
            Files.setPosixFilePermissions(path, perms);
        } catch (IOException e) {
            throw new CacheException("Error while setting permissions", e);
        }
    }

    private Path resolveKey(String key) throws CacheException {
        return mpath.resolve("data").resolve(hash(key));
    }
}

