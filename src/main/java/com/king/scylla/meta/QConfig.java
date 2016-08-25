// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.meta;

import com.king.scylla.LogColouriser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class QConfig {
    private ScyllaConf conf;

    private String query;

    private Scope scope;
    private String user;
    private String password = "";

    private int expire = 86400;

    private boolean force = false;
    private boolean quiet = false;

    private boolean update = false;

    private List<String> hparams;

    private boolean peek = false;

    private static String[] booleanFields = new String[]{"force", "quiet", "update", "peek", "reckless"};

    private LogColouriser logColouriser;

    private String errorMessage;
    private String JDBCString;

    public QConfig(Scope scope, String query, String user) {
        this.query = query;
        this.scope = scope;
        this.user = user;
    }

    public ScyllaConf getConf() {
        return conf;
    }

    public void setConf(ScyllaConf conf) {
        this.conf = conf;
    }

    public String getQuery() {
        return query;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean isQuiet() {
        return quiet;
    }

    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public List<String> getHParams() {
        return hparams;
    }

    public void setHParams(List<String> hparams) {
        this.hparams = hparams;
    }


    public LogColouriser getLogColouriser() {
        return logColouriser;
    }

    public void setLogColouriser(LogColouriser logColouriser) {
        this.logColouriser = logColouriser;
    }

    public boolean isPeek() {
        return peek;
    }

    public void setPeek(boolean peek) {
        this.peek = peek;
    }

    public String getJDBCString() {
        return JDBCString != null ? JDBCString : getConf().getDefaultJDBCStringForSope(scope);
    }

    private void setJDBCString(String JDBCString) {
        this.JDBCString = JDBCString;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    private static void validateStringParam(JSONObject object, String key) throws JSONException {
        object.getString(key);
    }

    private static void validateIntParam(JSONObject object, String key) throws JSONException {
        object.getInt(key);
    }

    private static void validateBooleanParam(JSONObject object, String key) throws JSONException {
        object.getBoolean(key);
    }

    private static void validateStringArrayParam(JSONObject object, String key) throws JSONException {
        object.getJSONArray(key);

        JSONArray a = object.getJSONArray(key);
        for (int i = 0; i < a.length(); i++) {
            // this is to check that every element of the array is a string
            String v = (String) a.get(i);
        }
    }

    // this isn't that great. it returns null if all ends well, and an error message otherwise.
    // TODO: think of a better solution
    private static String checkForErrors(JSONObject o) {
        String field = null;

        try {
            field = "query";
            validateStringParam(o, field);

            field = "user";
            validateStringParam(o, field);
            if (o.has("scope")) {
                field = "scope";
                validateStringParam(o, field);
                Scope scope = Scope.getScopeFromShortName(o.getString("scope"));
                if (scope.needsPassword() && !o.has("password")) {
                    return "I needed a password but I got none!";
                }
                if (o.has("password")) {
                    field = "password";
                    validateStringParam(o, field);
                }
            }

            if (o.has("expire")) {
                field = "expire";
                validateIntParam(o, field);
                if (o.getInt(field) <= 0) {
                    return "'expire' must be greater than zero, realistically a lot greater ...";
                }
            }

            for (String booleanField : QConfig.booleanFields) {
                if (o.has(booleanField)) {
                    field = booleanField;
                    validateBooleanParam(o, booleanField);
                }
            }

            if (o.has("jdbcstring")) {
                field = "jdbcstring";
                validateStringParam(o, field);
            }

            if (o.has("hparams")) {
                field = "hparams";
                validateStringArrayParam(o, field);
            }
        } catch (JSONException e) {
            return String.format("Field %s has the wrong type!", field);
        }
        return null;
    }

    public static QConfig qcFromInstruction(JSONObject instruction, LogColouriser logColouriser) {
        String errm = checkForErrors(instruction);
        QConfig qc;
        if (errm != null) {
            qc = new QConfig(null, null, null);
            qc.setErrorMessage(errm);
            return qc;
        } else {
            String sscope = "hive";
            String user = instruction.getString("user");

            qc = new QConfig(Scope.getScopeFromShortName(sscope), instruction.getString("query"), user);
            qc.setLogColouriser(logColouriser);

            qc.setPassword("");
            qc.setExpire(86400);

            qc.setForce(false);
            qc.setQuiet(false);
            qc.setUpdate(false);

            if (instruction.has("expire")) {
                qc.setExpire(instruction.getInt("expire"));
            }
            if (instruction.has("force")) {
                qc.setForce(instruction.getBoolean("force"));
            }
            if (instruction.has("quiet")) {
                qc.setQuiet(instruction.getBoolean("quiet"));
            }
            if (instruction.has("update")) {
                qc.setUpdate(instruction.getBoolean("update"));
            }
            if (instruction.has("peek")) {
                qc.setPeek(instruction.getBoolean("peek"));
            }
            if (instruction.has("scope")) {
                sscope = instruction.getString("scope");
                qc.setScope(Scope.getScopeFromShortName(sscope));
            }
            if (instruction.has("jdbcstring") && (instruction.getString("jdbcstring") != null)) {
                qc.setJDBCString(instruction.getString("jdbcstring"));
            }
            if (instruction.has("password") && (instruction.getString("password") != null)) {
                qc.setPassword(instruction.getString("password"));
            }

            if (instruction.has("hparams")) {
                List<String> hparams = new ArrayList<>();
                for (int i = 0; i < instruction.getJSONArray("hparams").length(); i++) {
                    hparams.add((String) instruction.getJSONArray("hparams").get(i));
                }
                qc.setHParams(hparams);
            }
        }

        return qc;
    }
}
