// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import com.king.scylla.meta.QConfig;
import com.king.scylla.meta.ScyllaException;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/*
    This is the essence of Scylla's answers. Modifying this means modifying Scylla's API so any drastic changes to
    be made here should be very important.

    Fields with obscure names (apologies) are documented.

    This object has a Scala-ish style which I'll probably replicate elsewhere.
 */
public class Answer {
    protected JSONObject msg;

    public enum Status {DONE, PENDING, PEEK, LOCKED}

    public enum PeekStatus {YES, NO, LOCKED}

    protected Answer() {
        msg = new JSONObject();
    }

    private Answer(JSONObject msg) {
        this.msg = msg;
    }

    public String toString() {
        return msg.toString();
    }

    // ok == false  ->  there is an error somewhere (the 'err' field has the info).
    public Answer ok(boolean ok) {
        msg.put("ok", ok ? "yes" : "no");
        return this;
    }

    // error message (only if ok == false)
    public Answer err(String err) {
        msg.put("err", err);
        return this;
    }

    public boolean hasErr() {
        return msg.has("err");
    }

    public String getErr() {
        return hasErr() ? msg.getString("err") : null;
    }

    // status of the query (pending, done, etc.; this value is never checked directly
    public Answer status(Status status) {
        msg.put("status", status.toString().toLowerCase());
        return this;
    }

    // this stores the peek result (yes: dataset available, no: no dataset available, pending: no dataset available but
    // there is a running query).
    public Answer peek(PeekStatus peek) {
        msg.put("peek", peek.toString().toLowerCase());
        return this;
    }

    public Answer n(int rows) {
        msg.put("n", rows);
        return this;
    }

    public Answer update(boolean update) {
        msg.put("update", update ? "yes" : "no");
        return this;
    }

    public boolean isDone() {
        return msg.has("res") || msg.has("n");
    }

    public static Answer emptyAnswer() {
        return new Answer();
    }

    public static Answer answerFromJSONObject(JSONObject jo) {
        return new Answer(jo);
    }

    /*
     * this builds the actual answer containing the data. a JSON object containing two fields:
     * 'cols': array column names
     * 'res': a base64 blob that once decoded and decompressed (bz2) is a JSON object that can be loaded from
     *        pandas.
     */
    public static Answer answerFromResultSet(QConfig qc, ResultSet rs)
            throws SQLException, JSONException, IOException, ScyllaException {
        Logger log = LogManager.getLogger(Answer.class.getName());
        ResultSetMetaData rsmd = rs.getMetaData();
        LogColouriser logc = qc.getLogColouriser();

        JSONObject mo = new JSONObject();

        ByteArrayOutputStream w = new ByteArrayOutputStream();
        Base64OutputStream b64os = new Base64OutputStream(w);
        BZip2CompressorOutputStream bz = new BZip2CompressorOutputStream(b64os);

        bz.write("[".getBytes("UTF-8"));

        JSONArray cols = new JSONArray();

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            cols.put(rsmd.getColumnLabel(i).toLowerCase());
        }

        long j = 0;

        while (rs.next()) {
            int numColumns = rsmd.getColumnCount();
            JSONObject obj = rowToJSON(rs);

            if (j > 0) {
                bz.write(",".getBytes("UTF-8"));
            }
            bz.write(obj.toString().getBytes("UTF-8"));

            j++;
            if (j * numColumns >= 150000000) {
                w.close();
                bz.close();
                b64os.close();
                throw new ScyllaException("Your result set is too big. Please add a limit or try getting the data " +
                        "some other way (e.g. create a table and export it to a CSV file manually).");
            }
        }

        bz.write("]".getBytes("UTF-8"));

        w.close();
        bz.close();
        b64os.close();

        log.debug(logc.cuteLog(qc.getUser(), String.format("Successfully fetched %d lines", j)));

        mo.put("cols", cols);
        mo.put("res", w.toString("UTF-8"));

        return new Answer(mo);
    }

    /*
     * this returns the useless answer for (successful) update statements, only containing one field.
     * 'n': number of rows affected
     */
    public static Answer answerForUpdateStatement(int rows)
            throws SQLException, JSONException, IOException {
        return new Answer().n(rows);
    }

    private static JSONObject rowToJSON(ResultSet rs) throws SQLException {
        JSONObject obj = new JSONObject();
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();

        for (int i = 1; i < numColumns + 1; i++) {
            String column_name = rsmd.getColumnName(i).toLowerCase();

            if (rs.getObject(column_name) == null) {
                obj.put(column_name, JSONObject.NULL);
            } else if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
                obj.put(column_name, rs.getArray(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
                obj.put(column_name, rs.getLong(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
                obj.put(column_name, rs.getBoolean(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
                obj.put(column_name, rs.getBlob(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
                obj.put(column_name, rs.getDouble(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
                obj.put(column_name, rs.getFloat(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
                obj.put(column_name, rs.getLong(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
                obj.put(column_name, rs.getNString(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
                obj.put(column_name, rs.getString(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
                obj.put(column_name, rs.getInt(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
                obj.put(column_name, rs.getInt(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
                obj.put(column_name, rs.getDate(column_name));
            } else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
                obj.put(column_name, rs.getTimestamp(column_name));
            } else {
                obj.put(column_name, rs.getObject(column_name));
            }
        }

        return obj;
    }
}
