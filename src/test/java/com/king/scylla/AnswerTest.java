// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import com.google.common.base.Charsets;
import com.king.scylla.meta.QConfig;
import com.king.scylla.meta.ScyllaException;
import com.mockrunner.mock.jdbc.MockResultSet;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;

import static com.king.scylla.Answer.answerFromResultSet;
import static com.king.scylla.VerificationAnswer.emptyVerificationAnswer;
import static com.king.scylla.meta.Scope.HIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnswerTest {
    @Test
    public void emptyAnswerTest() {
        assertEquals(Answer.emptyAnswer().toString(), "{}");
    }

    @Test
    public void answerWithErrorTest() {
        String errm = "error message";
        Answer answer = Answer.emptyAnswer().ok(false).err(errm);

        assertTrue(answer.hasErr());
        assertEquals(answer.getErr(), errm);
    }

    @Test
    public void verificationAnswerTest() {
        VerificationAnswer va = emptyVerificationAnswer().nobg(true);
        assertTrue(va.nobg());

    }

    private ResultSet prepareResultSet() throws ParseException {
        MockResultSet rs = new MockResultSet("test");

        rs.addColumn("intc", new Object[]{1, 2, 3});
        rs.addColumn("floatc", new Object[]{1.2, 2.4, 3.6});
        rs.addColumn("strc", new Object[]{"a", "b", "c"});
        rs.addColumn("datec", new Object[]{
                new DateTime("2016-01-01", DateTimeZone.UTC),
                new DateTime("2016-01-02", DateTimeZone.UTC),
                new DateTime("2016-01-03", DateTimeZone.UTC),
        });

        return rs;
    }

    private QConfig prepareDummyQConfig() {
        QConfig qc = new QConfig(HIVE, "something", "user");
        qc.setLogColouriser(new LogColouriser());
        return qc;
    }

    private JSONArray decompressRes(String res) throws IOException {
        byte[] resb = Base64.decodeBase64(res);

        ByteArrayInputStream is = new ByteArrayInputStream(resb);
        BZip2CompressorInputStream bzr = new BZip2CompressorInputStream(is);

        byte[] decompressed = new byte[666];
        int n = bzr.read(decompressed, 0, 666);

        String rawres = new String(decompressed, Charsets.UTF_8);

        return new JSONArray(rawres);
    }

    @Test
    public void answerFromResultSetTest() throws SQLException, IOException, ScyllaException, ParseException {
        ResultSet rs = prepareResultSet();
        QConfig qc = prepareDummyQConfig();

        Answer answer = answerFromResultSet(qc, rs);

        assertTrue(answer.isDone());

        JSONObject msg = answer.msg;

        assertTrue(msg.has("res"));

        JSONArray res = decompressRes(msg.getString("res"));

        assertEquals(res.length(), 3);

        JSONObject firstRow = res.getJSONObject(0);
        JSONObject secondRow = res.getJSONObject(1);
        JSONObject thirdRow = res.getJSONObject(2);

        assertEquals(firstRow.getDouble("floatc"), 1.2, 0.001);
        assertEquals(secondRow.getString("strc"), "b");
        assertEquals(thirdRow.getInt("intc"), 3);

        // the following part is a bit artificial because it will depend on each ResultSet's implementation of the
        // date type but at least it seems self-consistent.

        DateTime dt = new DateTime(firstRow.getString("datec"));

        assertEquals(dt.getYear(), 2016);
        assertEquals(dt.getMonthOfYear(), 1);
        assertEquals(dt.getDayOfMonth(), 1);
    }
}
