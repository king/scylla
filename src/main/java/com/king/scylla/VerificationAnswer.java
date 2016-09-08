// Copyright (C) king.com Ltd 2016
// https://github.com/king/scylla
// License: Apache 2.0, https://raw.github.com/king/scylla/LICENSE-APACHE

package com.king.scylla;

public class VerificationAnswer extends Answer {
    // this field is set in case the verification step detects it's going to be an immediate query (e.g. describe,
    // show tables, etc.)
    public VerificationAnswer nobg(boolean nobg) {
        msg.put("nobg", nobg ? "yes" : "no");
        return this;
    }

    boolean nobg() {
        return msg.has("nobg") && msg.get("nobg").equals("yes");
    }

    public static VerificationAnswer emptyVerificationAnswer() {
        return new VerificationAnswer();
    }
}
