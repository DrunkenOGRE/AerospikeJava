package io.dogre.aerospike;

import com.aerospike.client.command.Buffer;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public class SocketServer implements Server {

    private int port;

    private ServiceHandler serviceHandler;

    public SocketServer(int port, ServiceHandler serviceHandler) {
        this.port = port;
        this.serviceHandler = serviceHandler;
    }

    @Override
    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.port);
            while (true) {
                Socket socket = serverSocket.accept();
                Thread thread = new Thread(new RequestHandler(socket, this.serviceHandler));
                thread.start();
            }
        } catch (Exception e) {
            System.err.print("Thread " + Thread.currentThread().getId() + " : ");
            e.printStackTrace(System.err);
        }
    }

    private static class RequestHandler implements Runnable {

        private Socket socket;

        private ServiceHandler serviceHandler;

        public RequestHandler(Socket socket, ServiceHandler serviceHandler) {
            this.socket = socket;
            this.serviceHandler = serviceHandler;
        }

        @Override
        public void run() {
            byte[] sizeHeader = new byte[8];
            try (InputStream in = this.socket.getInputStream(); OutputStream out = this.socket.getOutputStream();
                    Socket socket = this.socket) {
                while (true) {
                    Arrays.fill(sizeHeader, (byte) 0);
                    int offset = 0;
                    int remains = 8;
                    while (0 < remains) {
                        int read = in.read(sizeHeader, offset, remains);
                        offset += read;
                        remains -= read;
                    }
                    long sizeHeaderValue = Buffer.bytesToLong(sizeHeader, 0);

                    int version = sizeHeader[0];
                    int type = sizeHeader[1];
                    int length = (int) (sizeHeaderValue & 0xffffffffffffL);

                    System.out.println(
                            "Thread " + Thread.currentThread().getId() + " : version = " + version + ", type = " +
                                    type + ", request length = " + length);
                    if (length == 0) {
                        break;
                    }

                    byte[] request = new byte[8 + length];
                    System.arraycopy(sizeHeader, 0, request, 0, 8);
                    remains = length;
                    while (0 < remains) {
                        int read = in.read(request, offset, remains);
                        offset += read;
                        remains -= read;
                    }

                    byte[] response = this.serviceHandler.handleRequest(request);

                    System.out.println(
                            "Thread " + Thread.currentThread().getId() + " : response length = " + response.length);

                    out.write(response);
                    out.flush();
                }
            } catch (Exception e) {
                System.err.print("Thread " + Thread.currentThread().getId() + " : ");
                e.printStackTrace(System.err);
            }
        }

    }

}
