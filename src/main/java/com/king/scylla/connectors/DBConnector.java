// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla.connectors;

import com.king.scylla.Answer;
import com.king.scylla.VerificationAnswer;
import com.king.scylla.meta.QConfig;
import com.king.scylla.meta.ScyllaException;
import org.json.JSONException;

import java.io.IOException;
import java.sql.*;

import static com.king.scylla.VerificationAnswer.emptyVerificationAnswer;
import static com.king.scylla.meta.Scope.*;

public class DBConnector {
    final QConfig qc;
    private final String connectorClass;
    private final String connectionString;

    Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(connectorClass);

        return DriverManager.getConnection(connectionString, qc.getUser(), qc.getPassword());
    }

    private VerificationAnswer verifyQueryWithExplain() throws SQLException, ScyllaException {
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
            throw new ScyllaException(qc.getScope().classNotFound());
        }
    }

    private VerificationAnswer verifyQueryWithPreparedStatement() throws SQLException, ScyllaException {
        try {
            Connection conn = getConnection();

            VerificationAnswer answer = emptyVerificationAnswer();
            try {

                Statement stmt = conn.createStatement();
                if (qc.getHParams() != null) {
                    for (String param : qc.getHParams()) {
                        stmt.execute(param);
                    }
                }

                PreparedStatement pstmt = conn.prepareStatement(qc.getQuery());
                pstmt.close();
                answer.ok(true);
            } catch (SQLException e) {
                answer.ok(false).err(e.getMessage());
            }
            conn.close();
            return answer;
        } catch (ClassNotFoundException e) {
            throw new ScyllaException(qc.getScope().classNotFound());
        }
    }

    public VerificationAnswer verifyQuery() throws SQLException, ScyllaException {
        if (qc.getScope() == HIVE || qc.getScope() == IMPALA) {
            return verifyQueryWithExplain();
        }
        else {
            return verifyQueryWithPreparedStatement();
        }
    }

        private Answer getAnswerFromStatement(Statement stmt, boolean update)
            throws SQLException, IOException, ScyllaException {
        if (update) {
            int n = stmt.executeUpdate(qc.getQuery());
            return Answer.answerForUpdateStatement(n);
        } else {
            ResultSet rs = stmt.executeQuery(qc.getQuery());
            return Answer.answerFromResultSet(qc, rs);
        }
    }

    public Answer query(boolean update) throws SQLException, JSONException, IOException, ScyllaException {
        try {
            Connection conn = getConnection();
            assert !conn.isClosed();

            Statement stmt = conn.createStatement();

            if (qc.getHParams() != null) {
                for (String param : qc.getHParams()) {
                    stmt.execute(param);
                }
            }

            Answer answer = getAnswerFromStatement(stmt, update);

            stmt.close();
            conn.close();

            return answer;
        } catch (ClassNotFoundException e) {
            throw new ScyllaException(qc.getScope().classNotFound());
        }
    }

    public DBConnector(QConfig qc) throws ScyllaException {
        this.qc = qc;
        this.connectionString = qc.getJDBCString();
        this.connectorClass = qc.getScope().getConnectorClass();

        if (this.connectionString == null) {
            throw new ScyllaException("No JDBC string provided explicitly and no default one configured.");
        }
    }
}
