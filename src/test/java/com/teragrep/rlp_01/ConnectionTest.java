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
    private Selector selector;
    private Thread thread;

    @BeforeEach
    public void testConnectionTeardown() throws IOException {
        this.thread = new Thread(this);
        System.setProperty("syslog.server.host", "127.0.0.1");
        System.setProperty("syslog.server.port", "1236");
        System.setProperty("syslog.server.protocol", "TCP");
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
            ServerSocket serverSocket = new ServerSocket(1236);
            Socket socket = serverSocket.accept();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while((line=bufferedReader.readLine())!=null && !line.isEmpty());
            // Server opening message
            socket.getOutputStream().write(("1 rsp " + reply.length() + " " + reply + "\n").getBytes());
            // Kill server immediately after sending message causing commit to fail
            socket.close();
            serverSocket.close();

            // Handle second connect
            serverSocket = new ServerSocket(1236);
            socket = serverSocket.accept();
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while((line=bufferedReader.readLine())!=null && !line.isEmpty());
            // Server opening message
            socket.getOutputStream().write(("1 rsp " + reply.length() + " " + reply + "\n").getBytes());
            socket.getInputStream().read();
            // First RelpBatch message
            socket.getOutputStream().write("2 rsp 6 200 OK\n".getBytes());
            // Disconnection message
            socket.getOutputStream().write("3 rsp 6 200 OK\n".getBytes());
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTeardown() throws IOException, TimeoutException {
        RelpConnection relpConnection = new RelpConnection();
        // First connection
        relpConnection.connect("127.0.0.1", 1236);

        // Server socket is closed by this point
        RelpBatch relpBatch = new RelpBatch();
        relpBatch.insert("Sending a message".getBytes(StandardCharsets.UTF_8));
        try {
            relpConnection.commit(relpBatch);
            Assertions.fail("Commit should fail");
        } catch(IOException ignored) {
            relpConnection.tearDown();
        }

        // New server socket is avaialble for second connection
        relpConnection.connect("127.0.0.1", 1236);
        relpBatch = new RelpBatch();
        relpBatch.insert("Sending a message".getBytes(StandardCharsets.UTF_8));
        relpConnection.commit(relpBatch);
        relpConnection.disconnect();
        Assertions.assertTrue(relpBatch.verifyTransactionAll(), "verifyTransactionAll() failed");
    }
}
