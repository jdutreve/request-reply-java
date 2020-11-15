import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * Thrift serialization: 890 K req/s
 * Thrift serialization: 384 K req/s
 */
final public class FreelanceClientThrift implements IFreelanceClient {

    static short REQUEST_RETRIES = 5;

    static String HEARTBEAT = "";
    static int HEARTBEAT_INTERVAL = 500; // milliseconds
    //  If no server replies within this time; abandon request
    static int HEARTBEAT_LIVENESS = HEARTBEAT_INTERVAL * 3;

    static int OUTBOUND_QUEUE_SIZE = 300_000;    // Queue to call external servers

    static int BATCH_NB = 5_000;

    FreelanceAgent agent;


    public FreelanceClientThrift() {
        this.agent = new FreelanceAgent();
    }

    public void startAgent() {
        try {
            this.agent.read_replies_send_requests();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stopAgent() {
        this.agent.isAlive = false;
    }

    public void connect(String endpoint) throws InterruptedException {
        this.agent.on_command_message(endpoint);
        Thread.sleep(100L);
    }

    public boolean sendRequest(int request_id, String request) {
        Object[] item = {request_id, request};
        return this.agent.request_queue.offer(item);
    }

    public Object[] receiveReply() {
        return this.agent.reply_queue.poll();
    }

    static class Request {
        int msg_id;
        String msg;
        short left_retries;
        Instant expires;

        public Request(int msg_id, String msg, Instant now) {
            this.msg_id = msg_id;
            this.msg = msg;
            this.left_retries = REQUEST_RETRIES;
            this.expires = now.plusSeconds(3);
        }
    }

    static class Server {
        String address;
        boolean alive;
        boolean connected;
        boolean is_last_operation_receive;
        Instant ping_at;
        Instant expires;

        public Server(String address) {
            this.address = address;
            this.alive = false;
            this.connected = false;
            this.is_last_operation_receive = false;
            this.ping_at = Instant.now().plusMillis(HEARTBEAT_INTERVAL);
            this.expires = Instant.now().plusMillis(HEARTBEAT_LIVENESS);
        }
    }

    static class FreelanceAgent {
        public static final Flow FLOW = new Flow("68718", "orderId", "Sales", "EMEA", "Region");
        public static final User USER = new User("system-msg-user@BA-FR", "email", "BA-FR", null);
        public static final Entity ENTITY = new Entity("98658097", "invoiceId", "Invoice");
        public static final Operation OPERATION = new Operation().setName("368a").setServiceName("omsf").setEnvironment("development");
        //        LinkedBlockingQueue<Object[]> request_queue;
//        LinkedBlockingQueue<Object[]> reply_queue;
        ArrayBlockingQueue<Object[]> request_queue;
        ArrayBlockingQueue<Object[]> reply_queue;
//        ConcurrentLinkedQueue<Object[]> request_queue;
//        ConcurrentLinkedQueue<Object[]> reply_queue;
        ZMQ.Socket backend_socket;
        Map<String, Server> servers;    //  Servers we've connected to
        List<Server> actives;           //  Servers we know are alive
        Map<Integer, Request> requests;
        int reply_nb;
        int received_nb;
        int failed_nb;
        String address;
        boolean isAlive;
        ZContext context;
        final TSerializer tSerializer = new TSerializer();
        final TDeserializer tDeserializer = new TDeserializer();
        final Message reply = new Message();

        public FreelanceAgent() {
//            request_queue = new LinkedBlockingQueue<Object[]>(1_000_000);
//            reply_queue = new LinkedBlockingQueue<Object[]>(1_000_000);
            request_queue = new ArrayBlockingQueue<Object[]>(1_000_000, false);
            reply_queue = new ArrayBlockingQueue<Object[]>(1_000_000, false);
//            request_queue = new ConcurrentLinkedQueue<Object[]>();
//            reply_queue = new ConcurrentLinkedQueue<Object[]>();
            reply_nb = 1;
            received_nb = 0;
            failed_nb = 0;
            servers = new HashMap<>();
            actives = new ArrayList<>();
            requests = new HashMap<>();
            address = "";
            isAlive = true;

            context = new ZContext();
            this.backend_socket = context.createSocket(ZMQ.ROUTER);
            this.backend_socket.setRouterMandatory(true);
            this.backend_socket.setSndHWM(OUTBOUND_QUEUE_SIZE);
            this.backend_socket.setRcvHWM(OUTBOUND_QUEUE_SIZE);
        }

        public void on_command_message(String endpoint) {
            System.out.println(String.format("I = connecting to %s", endpoint));
            this.backend_socket.connect(endpoint);
            this.servers.put(endpoint, new Server(endpoint));
        }

        public int on_request_message(Instant now) throws Exception {
            final Object[] item = this.request_queue.poll();
            if (item == null) {
                return -1;
            }
            final int request_id = (int)item[0];
            final Request request = new Request(request_id, (String)item[1], now);
            this.requests.put(request_id, request);
            this.send_request(request, now);
            return request_id;
        }

        public void send_request(Request request, Instant now) throws Exception {
            OPERATION.setTimestamp(now.toEpochMilli()).setDurationUs(5013630);
            final Message message = new Message(
                    request.msg_id,
                    request.msg,
                    "Command",
                    now.toEpochMilli(),
                    request.msg_id,
                    FLOW,
                    USER,
                    ENTITY,
                    OPERATION
            );
            this.backend_socket.send(this.address, ZMQ.DONTWAIT | ZMQ.SNDMORE);
            this.backend_socket.send(tSerializer.serialize(message), ZMQ.DONTWAIT);
            //System.out.println(String.format("send_request(%d)", request.msg_id));
        }

        public int on_reply_message(Instant now) throws Exception {
            // ex: reply = [b'tcp://192.168.0.22:5555', b'157REQ124'] or [b'tcp://192.168.0.22:5555', b'']
            final byte[] hostname = this.backend_socket.recv(ZMQ.DONTWAIT);
            if (hostname == null) {
                return -1;
            }
            String server_hostname = new String(hostname);
            final Server server = this.servers.get(server_hostname);
            server.is_last_operation_receive = true;
            server.ping_at = now.plusMillis(HEARTBEAT_INTERVAL);
            server.expires = now.plusMillis(HEARTBEAT_LIVENESS);

            final byte[] data = this.backend_socket.recv(ZMQ.DONTWAIT);
            if (data.length == 0) {
                // p("I = RECEIVE PONG   %s" % server_hostname)
                server.connected = true;
            } else {
                received_nb++;
                tDeserializer.deserialize(reply, data);
                final Request request = this.requests.get(reply.getId());
                if (request != null) {
                    this.send_reply(now, reply.getCorrelationId(), reply.getName(), server_hostname);
//                    System.out.println(String.format("on_reply_message(%d) : %s %s", received_nb, msg_id, msg));
                    //} else {
                    //pass
                    //p("W = TOO LATE REPLY  %s" % data[-msg_len:])
                }
            }

            if (!server.alive) {
                server.alive = true;
                System.out.println(String.format("I = SERVER ALIVE %s", server.address));
            }

            //this.mark_as_active(server)
            if (this.address.isEmpty()) {
                this.address = server.address;
                this.actives.add(server);
            }
            return 1;
        }

        public void send_reply(Instant now, int request_id, String reply, String server_name) {
            Request request = this.requests.remove(request_id);
            this.reply_nb++;
            //if (server_name == "") {
                //p("W = REQUEST FAILED  %s" % request_id)
            //} else {
                //p("I = RECEIVE REPLY  %s" % server_name)
            //}
            final Object[] item = {request.msg_id, reply};
            this.reply_queue.offer(item);
        }

//        public void mark_as_active(Server server) {
//            this.actives = {server};
//        }

        public void read_replies_send_requests() throws Exception {
            Instant now;
            int i, j = 0;

            while (isAlive) {
                now = Instant.now();

                for (i=0; i<BATCH_NB; i++) {
                    if (on_reply_message(now) < 0) {
                        break;
                    }
                }

                if (!actives.isEmpty()) {
                    for (j=0; j<BATCH_NB; j++) {
                        if (on_request_message(now) < 0) {
                            break;
                        }
                    }
                }

                if (i == 0 && j == 0) {
                    Thread.sleep(0, 1000);
                }
            }

            context.close();
        }
    }
}