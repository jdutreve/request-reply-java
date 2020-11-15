public interface IFreelanceClient {
    void startAgent();

    void stopAgent();

    void connect(String endpoint) throws InterruptedException;

    boolean sendRequest(int request_id, String request);

    Object[] receiveReply();
}
