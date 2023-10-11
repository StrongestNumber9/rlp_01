package com.teragrep.rlp_01;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ConnectionTest implements Runnable {
    private ServerSocket serverSocket;
    private Selector selector;
    private Thread thread;

    @BeforeEach
    public void testConnectionTeardown() throws IOException {
        this.thread = new Thread(this);
        System.setProperty("syslog.server.host", "127.0.0.1");
        System.setProperty("syslog.server.port", "1236");
        System.setProperty("syslog.server.protocol", "TCP");
        serverSocket = new ServerSocket(1236);
        selector = Selector.open();
        this.thread.start();
    }

    @AfterEach
    public void shutdown() {
        this.thread.interrupt();
    }

    @Override
    public void run() {
        String reply ="200 OK\nrelp_version=0\nrelp_software=RLP-01,1.0.1,https://teragrep.com\ncommands=syslog";
        try {
            // Handle first connect
            Socket socket = this.serverSocket.accept();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while((line=bufferedReader.readLine())!=null && !line.isEmpty());
            socket.getOutputStream().write(("1 rsp " + reply.length() + " " + reply + "\n").getBytes());
            socket.getInputStream().read();

            // Handle second connect
            socket = this.serverSocket.accept();
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while((line=bufferedReader.readLine())!=null && !line.isEmpty());
            socket.getOutputStream().write(("1 rsp " + reply.length() + " " + reply + "\n").getBytes());
            socket.getInputStream().read();
            socket.getOutputStream().write("2 rsp 6 200 OK\n".getBytes());
            socket.getOutputStream().write("3 rsp 6 200 OK\n".getBytes());
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTeardown() throws IOException, TimeoutException {
        RelpConnection relpConnection = new RelpConnection();
        relpConnection.connect("127.0.0.1", 1236);
        relpConnection.tearDown();
        relpConnection.connect("127.0.0.1", 1236);
        RelpBatch relpBatch = new RelpBatch();
        relpBatch.insert("Sending a message".getBytes(StandardCharsets.UTF_8));
        relpConnection.commit(relpBatch);
        relpConnection.disconnect();
        Assertions.assertTrue(relpBatch.verifyTransactionAll(), "verifyTransactionAll() failed");
    }
}
