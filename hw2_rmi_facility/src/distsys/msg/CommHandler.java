package distsys.msg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 10/9/13
 */
public class CommHandler {

    private String hostname;
    private int port;

    private Socket sock;
    private ObjectInputStream sockIn;
    private ObjectOutputStream sockOut;

    /**
     * Class for simplifying the handling of RMI messages between hosts
     *
     * @param hostname Hostname to talk to
     * @param port     Port on receiving host
     * @throws IOException
     */
    public CommHandler(String hostname, int port) throws IOException {
        this.hostname = hostname;
        this.port = port;

        sock = new Socket(this.hostname, this.port);
    }

    /**
     * Class for simplifying the handling of RMI messages between hosts
     *
     * @param sock Socket you're connected to
     */
    public CommHandler(Socket sock) {
        this.sock = sock;
        this.hostname = sock.getInetAddress().getCanonicalHostName();
        this.port = sock.getPort();
    }


    /**
     * Send an RMI message to the connected host
     *
     * @param message RMI message to send
     * @throws IOException
     */
    public void sendMessage(RmiMessage message) throws IOException {
        // Write the message object to socket's output stream, flushing it to the connected host
        sockOut = new ObjectOutputStream(sock.getOutputStream());
        sockOut.writeObject(message);
        sockOut.flush();
    }

    /**
     * Receive an RMI message from the connected host
     *
     * @return Either the RMI message sent or an Exception message in the event of bad data
     * @throws IOException
     */
    public RmiMessage receiveMessage() throws IOException {
        // Listens to the connected inputstream on the socket, returning the RMI message received
        sockIn = new ObjectInputStream(sock.getInputStream());
        try {
            return (RmiMessage) sockIn.readObject();
        } catch (ClassNotFoundException e) {
            return new RmiExceptionMessage(e);
        }
    }

}
