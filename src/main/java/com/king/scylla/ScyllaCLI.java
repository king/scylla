// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

import com.king.scylla.meta.Format;
import com.king.scylla.meta.ScyllaConf;
import com.king.scylla.meta.ScyllaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ScyllaCLI {
    @Option(name = "--port", aliases = {"-p"}, usage = "-p 30666")
    private int port = 30666;

    @Option(name = "--config", aliases = {"-c"}, usage = "-c /etc/scylla.properties")
    private String config = "/etc/scylla.properties";

    @Option(name = "--format", aliases = {"-f"}, usage = "-f json")
    private String format = "csv";

    private final Logger log = LogManager.getLogger(ScyllaCLI.class.getName());

    private final ExecutorService pool = Executors.newFixedThreadPool(256);

    @SuppressWarnings("InfiniteLoopStatement")
    private void run(String[] args) throws IOException, CmdLineException, ScyllaException {
        CmdLineParser parser = new CmdLineParser(this);

        parser.parseArgument(args);

        ScyllaConf conf = new ScyllaConf(config).check();
        ServerSocket ss = new ServerSocket(port);

        if (conf.getFormat() == null) {
            conf.setFormat(Format.fromString(format));
        }

        log.info("Scylla says hi!");
        log.info(String.format("Waiting for connections on port %d ...", port));

        int i = 1;

        while (true) {
            Socket cs = ss.accept();
            cs.setSoTimeout(300000);
            log.debug("Cool, one client just connected!");
            String name = String.format("Tentacle %d [%s]", i, cs.getInetAddress().toString());
            new Thread(new Scylla(cs, pool, conf), name).start();
            i++;
        }
    }

    public static void main(String[] args) throws SQLException, JSONException, IOException, CmdLineException, ScyllaException {
        new ScyllaCLI().run(args);
    }
}
