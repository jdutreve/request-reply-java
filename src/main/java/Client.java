import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

public class Client
{
    static int REQUEST_NUMBER = 600_000;

    public static void main(String[] args) throws Exception
    {
        IFreelanceClient client = null;

        if (args.length == 0) {
            client = new FreelanceClientDefault();
            // client = new FreelanceClientMsgpack();
        } else if (args[0].equals("avro")) {
            client = new FreelanceClientAvro();
        } else if (args[0].equals("proto")) {
            client = new FreelanceClientProtobuf();
        } else if (args[0].equals("thrift")) {
            client = new FreelanceClientThrift();
        }

//        client.connect("ipc://server.ipc");
        client.connect("tcp://192.168.0.22:5555");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(client::startAgent);

        send_all_requests(client);
        read_all_replies(client);

        send_all_requests(client);
        read_all_replies(client);

        send_all_requests(client);
        read_all_replies(client);

        client.stopAgent();
        executor.shutdownNow();
    }

    static void send_all_requests(IFreelanceClient client) {
        for (int i = 1; i < REQUEST_NUMBER+1; i++) {
            client.sendRequest(i, "R€Q" + i);
            //print("SEND REQUESTS FINISHED")
        }
    }

    static void read_all_replies(IFreelanceClient client) throws InterruptedException {

        int reply_nb = 0;
        Object[] reply;

        while (true) {
            reply = client.receiveReply();

            if (reply == null) {
                sleep(0, 10_000);
            } else {
                reply_nb++;
//                System.out.println(String.format("%d %s %d", reply[0], reply[1], reply_nb));
                if (reply_nb == REQUEST_NUMBER) {
                    System.out.println("**************************** READ REPLIES FINISHED ****************************");
                    break;
                }
            }
        }
    }
}