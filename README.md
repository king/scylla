Scylla
======

A query scheduler with tentacles<sup>TM</sup>. Combined with a client you can do the following.

* Fire a Hive query from an interactive environment, do whatever you were doing without having to wait for the results
and whenever those are ready you can fire the query again to get them back.
* Disconnect your computer from the network, reconnect it, fire the query again and get the same results without waiting.
* Set your computer on fire, get a new one, reinstall everything, launch the exact same query you launched before
and still get your results back without waiting, as long as the cached results are recent enough.

It supports the following "databases":

* Hive
* Impala (installing the official JDBC driver)
* Redshift (as above)
* Exasol (as above)

Should I use it?
----------------

Scylla acts as an asynchronous, language-agnostic database connector, and it's great for interactive usage. If your
workflow includes launching arbitrary long-running queries from an interactive environment then Scylla offers great
advantages:

* You fire your query, your REPL doesn't get locked. You can relaunch it later to check for results, in the meantime
you can do whatever other things you normally do inside your REPL.
* You don't have intermediate CSV files lying around. Once you get your data set from Scylla you decide what to do.
Calling the same query will give you the same data (for as long as the cached set lasts), so "your query is your data
set".
* You don't need any persistent connection to your remote server where you would normally launch the Hive CLI from.

Some of these benefits are also advantages to, say, Python Hive clients such as
[impyla](https://github.com/cloudera/impyla), which is a good option but it requires steady connections and since
it's "just a database connector" it doesn't have additional functionality such as result set caching, etc.

In which cases should I not use it?
-----------------------------------

Two (common) tasks are not particularly good for Scylla.

* Exports of huge data sets. There is a *150M fields* explicit cap.
* Automatised jobs. This is technically doable but it's not recommended because there isn't an obvious way to figure
out whether something such as a `create table` statement is done or not.

This might change in the future (especially the first).

Also, keep in mind that Scylla caches the full data set so if your job involves streaming through your result sets
(e.g. through a `select * from ... where ... order by ...`) this will be an extra step, so it will be very inefficient.

How do I run it?
----------------

Compile everything with `mvn package` and you'll get a Debian package to install in your
favourite Linux machine, which, once installed, starts scylla as a service. To stop it:

`# systemctl stop scylla.service`

Scylla depends *at runtime* on the Impala, Exasol and Redshift JDBC drivers, which you need to have in your classpath
(`/usr/share/java/scylla` if you install the Debian package). If those drivers aren't found Scylla will
still work but you'll only have Hive and Impala.

You might need a configuration file (`/etc/scylla.properties` by default) with the following options:

### JDBC Strings

They're all optional and they can be overridden by setting `jdbcstring` explicitly in your queries.

* `hive_jdbcstring=...`
* `exasol_jdbcstring=...`
* `redshift_jdbcstring=...`
* `impala_jdbcstring=...`

### Other parameters

Again, all optional.

* `cache_path=/tmp/scylla.fcache`: Path for Scylla's cache
* `cache_lifetime_days=7`: Scylla's cache's lifetime (in days)

How does it work?
-----------------

You ask Scylla a question (via a socket), she will answer you and close the connection. Simple.

This is the most basic example of how this works:

```
$ telnet 127.0.0.1 30666
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
{"query": "desc dual", "user": "somebody"}
{"res":"QlpoOTFBWSZTWW1IW+8AAAibgBAEABAACq4nxCogACGoxTTJoz1PShTCaaA0xGqR4HgJsWoLzqIw\r\ndPQqrLt2KKE5F7ezn8XckU4UJBtSFvvA","ok":"yes","cols":["col_name","data_type","comment"],"status":"done"}
Connection closed by foreign host.
$
```

Questions must be minified one-line JSON strings with a trailing newline:

```python
# this format isn't obviously valid because it isn't minified, it's for educational purposes only.

{
    "user": "your_unix_user",        # required
    "query": "select ... from ...",  # also required
    "scope": "hive",                 # (optional) "hive", "exasol", "redshift" or "impala" (default is "hive")
    "password": "abc123",            # (optional for hive and impala, required for exasol and redshift): won't be stored anywhere
    "expire": 86400,                 # (optional) default is one day
    "force": False,                  # (optional) tells scylla to ignore cached results: default is false
    "quiet": False,                  # (optional) to be used in loops to simulate synchronous querying: default is false
    "update": False,                 # (optional) for update statements, if true queries won't return data sets; default is false
    "jdbcstring": "jdbc:hive2://..." # (optional) JDBC string for the current query.
}
```

Query results are cached server-side with the defined `expire` parameter (there is a hard-coded cap of one week).

Answers with a `res` field (like the one in the tiny example above) have the data set compressed (`bz2`) and encoded to
base64. Decode, decompress and you'll get a [Pandas](http://pandas.pydata.org/)-friendly CSV or JSON object, depending
on what the `format` field looks like. Ideally, this piece of Python code is enough:

```python
import json
import bz2
import base64

import pandas as pd

jr = json.loads(answer.decode("utf-8")) # where `answer` is scylla's answer straight from a socket (a byte string)
with io.StringIO(jr["res"]) as f:
    df = pd.read_csv(io.StringIO(f), header=None, names=jr["cols"], sep="\t")
```

Possible questions and answers
------------------------------

**All answers are JSONs**.

**DO NOT USE MULTIPLE STATEMENTS, IT WON'T BE SUPPORTED**.

|Question|Answer|
|---|---|
|*Good instruction with a bad query (e.g.`{"user": "blah", "query": "lol"}`)*|A JSON with `"ok": "no"` and the `hive` error in the `"err"` field|
|*Malformed instruction, e.g.* missing user: *`{"query": "blah"}`,* wrong format: *`$$notajs0n0bject666`, etc.*|`{"ok": "no", "err": "Malformed instruction :("}`|
|*`desc` or `show tables`query*|A JSON with `"ok": "yes"` and the `desc` results (`"cols"` and `"res"` fields)|
|*Good instruction with a `select` statement*|There are four possible answers to this:<li>No cached results available: Scylla returns a JSON with `"ok": "yes"` and `"status": "pending"` and then launches the query in the background.<li>The query is running already but hasn't finished yet: Scylla returns a JSON with `"ok": "yes"` and `"status": "locked"`.<li>There is a cached result available: Scylla returns `"status": "done"` and the query results in the `"cols"` and `"res"` fields.<li>The query failed half-way (you killed the job or Hive is misbehaving): Scylla returns `"ok": "no"` and the error message in the `"err"` field.|

Other info
----------

* Don't use Scylla to query for hundreds of millions of lines.
* `quiet` is used for API's to simulate "sync" behaviour (e.g. you want to wait until your query is over: you launch
it once, if it works you might want to ask for results every 5-10 seconds but not spam the server with log messages,
so it ultimately only suppresses the "in-between" logs (e.g. the "pending" and the "good query" messages). Obviously
`quiet` doesn't mean that Scylla won't answer. Scylla always answers.
* The socket has a 5 min timeout. Scylla is designed to give quick answers (less than a second if no results are
involved).
* There is no monitoring system whatsoever aside from the log.

Security
--------

Scylla passes everything on to the JDBC connector. This means that if your database has no security of its own (e.g.
unauthenticated Hadoop cluster) it's technically possible to launch harmful queries, as it could be from any other tool
accessing the database or cluster (e.g. regular unauthenticated JDBC, webHDFS, etc.).

Another possible issue could be that since cached result sets aren't encrypted, if your database has restricted
schemas, those sets might be temporary accessible by everyone with access to the Scylla instance you're using, so if
this is a common scenario for you you should behave accordingly.

TODO
----

* Use SSL for the socket.
* Better monitoring.
* Fix the colouring logic as it's unbelievably intricate right now.