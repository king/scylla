// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import com.google.common.base.Throwables;
import com.king.scylla.cache.Cache;
import com.king.scylla.cache.CacheException;
import com.king.scylla.connectors.*;
import com.king.scylla.meta.QConfig;
import com.king.scylla.meta.Scope;
import com.king.scylla.meta.ScyllaConf;

import com.king.scylla.meta.ScyllaException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.Socket;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.king.scylla.Answer.Status.*;
import static com.king.scylla.Answer.PeekStatus;
import static com.king.scylla.Answer.answerFromJSONObject;
import static com.king.scylla.Answer.emptyAnswer;
import static com.king.scylla.LogColouriser.whiteOnBlack;
import static com.king.scylla.meta.Scope.HIVE;
import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;

public class Scylla implements Runnable {
    private static final Logger log = LogManager.getLogger(Scylla.class.getName());
    private final String addr;
    private final ExecutorService pool;
    private final ScyllaConf conf;

    private LogColouriser logColouriser;

    private DBConnector connector;

    private Socket socket;

    private String genKey(QConfig qc) {
        return "scylla|" + qc.getJDBCString() + "|" + qc.getQuery();
    }

    private String shorten(String query) {
        if (query.length() < 2000) {
            return escapeJson(query);
        } else {
            return escapeJson(query.substring(0, 1996) + "...");
        }
    }

    // this is a bit useless right now but it will make a lot of sense with different scylla replies implemented
    // besides bare data delivery and `peek`.
    private Answer getAnswer(QConfig qc) throws SQLException, IOException, CacheException, ScyllaException {
        if (!qc.isPeek()) {
            return getRealAnswer(qc);
        } else {
            return getPeekAnswer(qc);
        }
    }

    private Answer getPeekAnswer(QConfig qc) throws CacheException {
        Cache fc = conf.cache();
        String key = genKey(qc);

        Answer peekAnswer = emptyAnswer().ok(true).status(PEEK);

        if (fc.exists(key)) {
            peekAnswer.peek(PeekStatus.YES);
        } else if (fc.locked(key)) {
            peekAnswer.peek(PeekStatus.LOCKED);
        } else {
            peekAnswer.peek(PeekStatus.NO);
        }
        return peekAnswer;
    }

    private Answer getRealAnswer(QConfig qc) throws IOException, CacheException, SQLException,
            ScyllaException {
        boolean force = qc.isForce();
        boolean quiet = qc.isQuiet();
        boolean update = qc.isUpdate();

        Cache fc = conf.cache();
        Scope scope = qc.getScope();
        String query = qc.getQuery();
        String key = genKey(qc);

        if (fc.exists(key) && !force) {
            if (fc.locked(key)) {
                Answer answer = emptyAnswer().ok(true).status(LOCKED);
                if (!quiet) {
                    log.warn(logColouriser.cuteLog(qc.getUser(), String.format("There is a lock on query %s. Maybe it's " +
                            "already running? Check your favourite monitor.", whiteOnBlack(shorten(query)))));
                }
                return answer;
            } else {
                log.info(logColouriser.cuteLog(qc.getUser(),
                        String.format("Cached version of query '%s' found ... good!", whiteOnBlack(shorten(query)))));
                Answer answer = answerFromJSONObject(fc.get(key));
                if (answer.hasErr()) {
                    log.warn(logColouriser.cuteLog(qc.getUser(), String.format("Something went wrong with that query, " +
                            "relaunch maybe? (%s)", answer.getErr())));
                }
                return answer;
            }
        } else {
            if (force) {
                // not necessary but better to be safe than sorry
                fc.delete(key);
                log.info(logColouriser.cuteLog(qc.getUser(),
                        String.format("Ignoring cached version for %s '%s'. Querying %s.",
                        logColouriser.colorise("query"), whiteOnBlack(shorten(query)), scope.getName())));
            } else {
                log.info(logColouriser.cuteLog(qc.getUser(),
                        String.format("No cached version found for %s '%s'. Querying %s.",
                        logColouriser.colorise("query"), whiteOnBlack(shorten(query)), scope.getName())));
            }
            VerificationAnswer v = connector.verifyQuery();

            if (v.hasErr()) {
                log.error(logColouriser.cuteLog(qc.getUser(), String.format("Your query has some issues: %s", v.getErr())));
                return v;
            } else if (v.nobg()) {
                log.info(logColouriser.cuteLog(qc.getUser(), "You're asking for a 'desc' or a 'show tables', " +
                        "getting the data straight away ..."));
                return queryDB(qc);
            } else {
                Answer answer = emptyAnswer().ok(true).status(PENDING);
                if (update) {
                    log.info(logColouriser.cuteLog(qc.getUser(), "Working in the background ..."));
                    answer.update(true);
                } else {
                    log.info(logColouriser.cuteLog(qc.getUser(), "Querying in the background. Come back later :)"));
                }
                queryInTheBackground(qc);
                return answer;
            }
        }
    }

    private Answer queryDB(QConfig qc) throws SQLException, CacheException, IOException, ScyllaException {
        boolean update = qc.isUpdate();
        int expire = qc.getExpire();

        log.info(logColouriser.cuteLog(qc.getUser(), String.format("Launching your %s.", logColouriser.colorise("query"))));

        Cache fc = conf.cache();
        String key = genKey(qc);

        Answer answer = emptyAnswer();

        fc.set(key, answer.toString());
        fc.lock(key);

        try {
            answer = this.connector.query(update);
        } catch (JSONException | SQLException | IllegalStateException e) {
            log.info(logColouriser.cuteLog(qc.getUser(), String.format("Your %s didn't finish! Logging the error ... (%s)",
                    logColouriser.colorise("query"), e.getMessage())));
            fc.unlock(key);
            fc.delete(key);
            answer.ok(false).err(e.getMessage() != null ? e.getMessage() : "null");
            fc.set(key, answer.toString());
            if (e instanceof IllegalStateException) {
                fc.expire(key, 3 * 86400);
            } else {
                fc.expire(key, 20);
            }

            fc.cleanup();
            return answer;
        }

        fc.unlock(key);

        // if the object is empty (as per creation) delete everything!
        if (!answer.isDone()) {
            fc.delete(key);
        } else {
            log.info(logColouriser.cuteLog(qc.getUser(), String.format("Your %s finished!", logColouriser.colorise("query"))));
            answer.ok(true).status(DONE);

            if (update) {
                // if it's an update statement there's no reason to cache anything (it will most likely fail on
                // re-execution)
                fc.delete(key);
            } else {
                // if the response size of the answer is too big don't cache it.
                if(answer.resSize() < 1000000000) {
                    fc.set(key, answer.toString());
                } else {
                    log.warn("Answer is too big to be cached here.");
                    fc.delete(key);
                }

                int cacheLifetime = conf.getCacheLifeTimeDays() * 86400;
                fc.expire(key, expire > 0 ? Math.min(expire, cacheLifetime) : cacheLifetime);
            }
        }

        fc.cleanup();
        return answer;
    }

    private Future<?> queryInTheBackground(final QConfig qc)
            throws SQLException, CacheException, IOException, ScyllaException {
        return pool.submit(() -> queryDB(qc));
    }

    @Override
    public void run() {
        try {
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            PrintWriter ow = new PrintWriter(socket.getOutputStream(), true);

            String rawQuestion = br.readLine();

            this.logColouriser = rawQuestion != null ? new LogColouriser(rawQuestion) : new LogColouriser();

            if (rawQuestion == null) {
                ow.println(emptyAnswer().ok(false).err("I got an empty instruction!"));
            } else {
                try {
                    JSONObject instruction = new JSONObject(rawQuestion);

                    QConfig qc = QConfig.qcFromInstruction(instruction, logColouriser);

                    if (qc.getErrorMessage() != null) {
                        ow.println(emptyAnswer().ok(false).err(qc.getErrorMessage()));
                    } else {

                        if (!qc.isQuiet()) {
                            log.debug(logColouriser.cuteLog(qc.getUser(),
                                    String.format("Good question from %s, processing ...",
                                            this.addr)));
                        }

                        qc.setConf(conf);

                        if (qc.getScope() == HIVE && qc.getConf().supportsHive()) {
                            connector = new HiveConnector(qc);
                        } else if (qc.getConf().isSupported(qc.getScope())) {
                            connector = new DBConnector(qc);
                        } else {
                            throw new ScyllaException(String.format("Scope %s not configured! Check " +
                                            "'/etc/scylla.properties' and make sure the driver is installed!",
                                    qc.getScope()));
                        }

                        Answer a = getAnswer(qc);
                        ow.println(a.toString());
                    }
                } catch (JSONException e) {
                    ow.println(emptyAnswer().ok(false).err(e.getMessage()));
                    log.error(logColouriser.cuteLog(addr, String.format("Got a malformed instruction from %s (%s)", this.addr,
                            e.getMessage())));
                } catch (CacheException | NotImplementedException | ScyllaException | SQLException e) {
                    try {
                        ow.println(emptyAnswer().ok(false).err(e.getMessage()));
                    } catch (JSONException d) {
                        log.error(logColouriser.cuteLog(addr, "Unhandled error: " + d.getMessage()));
                    }
                    log.error(logColouriser.cuteLog(addr, "Unhandled error: " + e.getMessage()));
                }
            }
        } catch (IOException e) {
            log.error(Throwables.getStackTraceAsString(e));
        }

        try {
            log.debug(String.format("Closing connection from [%s].", LogColouriser.whiteOnPink(this.addr)));
            socket.close();
        } catch (IOException e) {
            log.error(Throwables.getStackTraceAsString(e));
        }
    }

    public Scylla(Socket s, ExecutorService pool, ScyllaConf conf) {
        this.socket = s;
        this.addr = socket.getInetAddress().toString();
        this.logColouriser = new LogColouriser();
        this.pool = pool;
        this.conf = conf;

        log.debug(String.format("Got connection from [%s].", LogColouriser.whiteOnPink(this.addr)));
    }
}
