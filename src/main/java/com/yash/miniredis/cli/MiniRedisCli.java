package com.yash.miniredis.cli;

import java.io.*;
import java.net.Socket;

/**
 * A dedicated interactive terminal client for mini-redis -- the same idea as
 * `redis-cli`. Talking to the server over raw `nc`/Python sockets works for
 * quick checks, but this gives a proper demo-able REPL: a persistent
 * connection, colored/formatted responses, command timing, and a couple of
 * client-side conveniences (HELP, CLEAR) that aren't part of the wire
 * protocol at all.
 *
 * Run it with:  java -cp out com.yash.miniredis.cli.MiniRedisCli [host] [port]
 */
public class MiniRedisCli {

    private static final String RESET = "\u001B[0m";
    private static final String BOLD = "\u001B[1m";
    private static final String DIM = "\u001B[2m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String GRAY = "\u001B[90m";

    public static void main(String[] args) throws IOException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6380;

        printBanner(host, port);

        Socket socket;
        BufferedReader in;
        BufferedWriter out;
        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            String banner = in.readLine();
            System.out.println(GREEN + "connected" + RESET + " " + DIM + "(" + banner + ")" + RESET + "\n");
        } catch (IOException e) {
            System.out.println(RED + "could not connect to " + host + ":" + port + " -- is the server running?" + RESET);
            System.out.println(DIM + "  " + e.getMessage() + RESET);
            return;
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        String lastCommand = null;

        while (true) {
            System.out.print(BOLD + CYAN + host + ":" + port + RESET + BOLD + "> " + RESET);
            System.out.flush();
            String line = stdin.readLine();
            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;

            String upper = line.toUpperCase();
            if (upper.equals("EXIT") || upper.equals("QUIT")) {
                try {
                    out.write("QUIT\n");
                    out.flush();
                    in.readLine();
                } catch (IOException ignored) {}
                break;
            }
            if (upper.equals("HELP")) {
                printHelp();
                continue;
            }
            if (upper.equals("CLEAR")) {
                System.out.print("\u001B[H\u001B[2J");
                System.out.flush();
                continue;
            }
            if (line.equals("!!")) {
                if (lastCommand == null) {
                    System.out.println(GRAY + "(no previous command)" + RESET);
                    continue;
                }
                line = lastCommand;
                System.out.println(DIM + "> " + line + RESET);
            }
            lastCommand = line;

            try {
                long start = System.nanoTime();
                out.write(line);
                out.write("\n");
                out.flush();
                String response = in.readLine();
                double ms = (System.nanoTime() - start) / 1_000_000.0;
                if (response == null) {
                    System.out.println(RED + "(connection closed by server)" + RESET);
                    break;
                }
                System.out.println(format(response) + GRAY + String.format("  (%.2fms)", ms) + RESET);
            } catch (IOException e) {
                System.out.println(RED + "connection error: " + e.getMessage() + RESET);
                break;
            }
        }

        try { socket.close(); } catch (IOException ignored) {}
        System.out.println(DIM + "connection closed." + RESET);
    }

    /** Colorizes a raw protocol reply the way redis-cli formats RESP replies. */
    private static String format(String response) {
        if (response.isEmpty()) return response;
        char tag = response.charAt(0);
        String body = response.substring(1);
        return switch (tag) {
            case '+' -> GREEN + body + RESET;
            case '-' -> RED + "(error) " + body + RESET;
            case ':' -> YELLOW + "(integer) " + body + RESET;
            case '$' -> body.equals("-1") ? GRAY + "(nil)" + RESET : CYAN + "\"" + body + "\"" + RESET;
            default -> response;
        };
    }

    private static void printBanner(String host, int port) {
        System.out.println(MAGENTA + BOLD);
        System.out.println("  __  __ _       _       ____          _ _     ");
        System.out.println(" |  \\/  (_)_ __ (_)     |  _ \\ ___  __| (_)___ ");
        System.out.println(" | |\\/| | | '_ \\| |_____| |_) / _ \\/ _` | / __|");
        System.out.println(" | |  | | | | | | |_____|  _ <  __/ (_| | \\__ \\");
        System.out.println(" |_|  |_|_|_| |_|_|     |_| \\_\\___|\\__,_|_|___/");
        System.out.println(RESET + DIM + "  a from-scratch, lock-striped, crash-recoverable KV store" + RESET);
        System.out.println(DIM + "  connecting to " + host + ":" + port + " ... type HELP for commands" + RESET + "\n");
    }

    private static void printHelp() {
        System.out.println(BOLD + "commands" + RESET);
        System.out.println("  " + CYAN + "SET" + RESET + " key value [EX seconds]   set a key, optional TTL");
        System.out.println("  " + CYAN + "GET" + RESET + " key                      fetch a key");
        System.out.println("  " + CYAN + "DEL" + RESET + " key                      delete a key");
        System.out.println("  " + CYAN + "EXPIRE" + RESET + " key seconds           set/refresh a TTL");
        System.out.println("  " + CYAN + "STATS" + RESET + "                        key count, stripe count");
        System.out.println("  " + CYAN + "PING" + RESET + "                         liveness check");
        System.out.println(BOLD + "\nclient-side" + RESET);
        System.out.println("  " + YELLOW + "!!" + RESET + "                          repeat last command");
        System.out.println("  " + YELLOW + "CLEAR" + RESET + "                       clear the screen");
        System.out.println("  " + YELLOW + "EXIT" + RESET + " / " + YELLOW + "QUIT" + RESET + "                 close the connection\n");
    }
}
