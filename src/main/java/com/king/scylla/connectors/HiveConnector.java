// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.connectors;

import com.king.scylla.Answer;
import com.king.scylla.VerificationAnswer;
import com.king.scylla.meta.QConfig;
import com.king.scylla.meta.ScyllaException;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static com.king.scylla.meta.Scope.HIVE;

public class HiveConnector extends DBConnector {
    private static final Logger log = LogManager.getLogger(HiveConnector.class.getName());

    @Override
    public VerificationAnswer verifyQuery() throws SQLException, JSONException, ScyllaException {
        try {
            // Use EXPLAIN instead of preparing the statement.
            String equery = "explain " + qc.getQuery();
            Connection conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(equery);
            VerificationAnswer answer = new VerificationAnswer();
            try {
                boolean nobg = false;

                if (qc.getHParams() != null) {
                    for (String param : qc.getHParams()) {
                        stmt.execute(param);
                    }
                }

                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    String exp = rs.getString(1);
                    if (exp.contains("Describe Table Operator") || exp.contains("Show Table Operator")) {
                        nobg = true;
                        break;
                    }
                }
                answer.ok(true);
                if (nobg) {
                    answer.nobg(true);
                }
            } catch (SQLException e) {
                answer.ok(false).err(e.getMessage());
            }
            conn.close();
            return answer;
        } catch (ClassNotFoundException e) {
            throw new ScyllaException(HIVE.classNotFound());
        }
    }

    @Override
    public Answer query(boolean update) throws SQLException, JSONException, IOException, ScyllaException {
        try {
            Connection conn = getConnection();
            assert !conn.isClosed();

            final HiveStatement stmt = (HiveStatement) conn.createStatement();

            Runnable logthread = () -> {
                while (stmt.hasMoreLogs()) {
                    try {
                        List<String> qlogs = stmt.getQueryLog();
                        if (!qlogs.isEmpty()) {
                            for (String s : qlogs) {
                                if (s.startsWith("DEBUG : ")) {
                                    log.debug(qc.getLogColouriser().cuteLog(qc.getUser(), s.substring(8)));
                                } else if (s.startsWith("INFO  : ")) {
                                    log.info(qc.getLogColouriser().cuteLog(qc.getUser(), s.substring(8)));
                                } else {
                                    log.info(qc.getLogColouriser().cuteLog(qc.getUser(), s));
                                }
                            }
                        }
                        Thread.sleep(1000);
                    } catch (SQLException | InterruptedException e) {
                        break;
                    }
                }
            };

            Thread thread = new Thread(logthread);
            thread.setName(String.format("HL %d", thread.getId()));
            thread.setDaemon(true);
            thread.start();

            ResultSet rs = null;

            if (qc.getHParams() != null) {
                for (String param : qc.getHParams()) {
                    stmt.execute(param);
                }
            }

            int n = 0;
            if (!update) {
                rs = stmt.executeQuery(qc.getQuery());
            } else {
                n = stmt.executeUpdate(qc.getQuery());
            }

            thread.interrupt();
            try {
                thread.join(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Answer answer;

            if (!update) {
                answer = Answer.answerFromResultSet(qc, rs);
            } else {
                answer = Answer.answerForUpdateStatement(n);
            }

            stmt.close();
            conn.close();
            return answer;
        } catch (ClassNotFoundException e) {
            throw new ScyllaException(HIVE.classNotFound());
        }
    }

    public HiveConnector(QConfig qc) throws ScyllaException {
        super(qc);
    }
}
