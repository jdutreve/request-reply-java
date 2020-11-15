import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import static org.zeromq.ZMQ.NOBLOCK;
import static org.zeromq.ZMQ.SNDMORE;

public class Server
{
    static final int NO_BLOCK_SND_MORE = NOBLOCK | SNDMORE;

    public static void main(String[] args) throws Exception {
        long start = 0;
        int counter = 0;
        boolean not_yet_started = true;
        byte[] request;
        byte[] hostname;
        int flags = NO_BLOCK_SND_MORE;

        try (ZContext context = new ZContext()) {
            ZMQ.Socket server_socket = context.createSocket(ZMQ.ROUTER);
            server_socket.setRcvHWM(1_000_000);
            server_socket.setSndHWM(1_000_000);
//            server_socket.setIdentity("ipc://server.ipc".getBytes());
            server_socket.setIdentity("tcp://192.168.0.22:5555".getBytes());
            server_socket.setProbeRouter(true);
            server_socket.setRouterMandatory(true);
            server_socket.bind("tcp://*:5555");
//            server_socket.bind("ipc://server.ipc");
            System.out.println("I: server is ready at tcp://192.168.0.22:5555");

            while (true) {
                hostname = server_socket.recv(flags);
                if (hostname != null) {
                    flags = NO_BLOCK_SND_MORE;
                    request = server_socket.recv(NOBLOCK);
                    if (request.length > 0) {
                        counter++;
                        server_socket.send(hostname, NO_BLOCK_SND_MORE);
                        server_socket.send(request, NOBLOCK);
                        if (not_yet_started) {
                            not_yet_started = false;
                            start = System.currentTimeMillis();
                        } else if (counter % 100_000 == 0) {
                            final long duration = System.currentTimeMillis() - start;
                            final long rate = counter / duration;
                            System.out.println("Count1: " + counter + ", Rate: " + rate);
                        }
                    } else {
                        System.out.println("PING: " + counter);
                        server_socket.send(hostname, NO_BLOCK_SND_MORE);
                        server_socket.send(request, NOBLOCK);
                    }
                } else {
                    flags = SNDMORE;
                }
            }
        }
    }
}
