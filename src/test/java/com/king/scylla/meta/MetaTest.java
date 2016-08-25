// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import org.apache.commons.lang.NotImplementedException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Properties;

import static com.king.scylla.meta.QConfig.qcFromInstruction;
import static com.king.scylla.meta.Scope.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class MetaTest {
    @Test
    public void scopeTest() {
        assertTrue(HIVE.getName().equals("Hive"));
        assertTrue(EXASOL.getName().equals("Exasol"));
        assertTrue(REDSHIFT.getName().equals("Redshift"));

        assertTrue(EXASOL.needsPassword());
        assertTrue(REDSHIFT.needsPassword());

        boolean valid = true;
        try {
            getScopeFromShortName("blah");
        } catch (NotImplementedException e) {
            valid = false;
        }
        assertFalse(valid);
    }


    @Test
    public void emptyScyllaConfTest() {
        ScyllaConf sc = new ScyllaConf(new Properties());
        boolean valid = true;
        try {
            sc.check();
        } catch (ScyllaException e) {
            valid = false;
        }
        assertFalse(valid);
    }

    @Test
    public void minimalScyllaConfTest() {
        Properties properties = new Properties();
        String ex = "example";
        properties.setProperty("hive_jdbcstring", ex);
        ScyllaConf sc = new ScyllaConf(properties);

        assertTrue(sc.getDefaultJDBCStringForSope(HIVE).equals("example"));
    }

    @Test
    public void minimalValidInstructionTest() throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("user", "test");
        jo.put("scope", "hive");
        jo.put("query", "sample query");

        QConfig qc = qcFromInstruction(jo, null);
        assertTrue(qc.getErrorMessage() == null);
    }

    @Test
    public void minimalInvalidInstructionTest() throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("user", "test");

        QConfig qc = qcFromInstruction(jo, null);
        assertFalse(qc.getErrorMessage() == null);
    }

    // missing password
    @Test
    public void subtleInvalidInstructiontest() throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("user", "test");
        jo.put("scope", "exasol");
        jo.put("query", "sample query");

        QConfig qc = qcFromInstruction(jo, null);
        assertFalse(qc.getErrorMessage() == null);
    }

    @Test
    public void defaultScopeInstructiontest() throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("user", "test");
        jo.put("query", "sample query");

        QConfig qc = qcFromInstruction(jo, null);
        assertTrue(qc.getScope().equals(HIVE));
    }
}
