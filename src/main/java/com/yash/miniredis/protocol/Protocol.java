package com.yash.miniredis.protocol;

/**
 * A deliberately simple, RESP-inspired text protocol -- newline-delimited,
 * human-typeable with `nc` or `telnet`, which makes the server trivial to
 * demo without a custom client.
 *
 * Requests:  SET key value [EX seconds] | GET key | DEL key | EXPIRE key seconds
 *            | PING | STATS | QUIT
 * Responses: +OK                 (simple success)
 *            -ERR message        (error)
 *            $value              (bulk string reply)
 *            $-1                 (nil / key not found)
 *            :123                (integer reply)
 */
public final class Protocol {
    private Protocol() {}

    public static String ok() { return "+OK"; }
    public static String pong() { return "+PONG"; }
    public static String err(String msg) { return "-ERR " + msg; }
    public static String nil() { return "$-1"; }
    public static String bulk(String value) { return "$" + value; }
    public static String integer(long n) { return ":" + n; }
}
