package distsys.msg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kevin, prashanth
 * Date: 11/6/13
 */
public class CommHandler implements Serializable {

    private final String hostname;
    private final int port;
    private Socket sock;

    /**
     * Class for simplifying the handling of RMI messages between hosts
     *
     * @param hostname Hostname to talk to
     * @param port     Port on receiving host
     * @throws java.io.IOException
     */
    public CommHandler(String hostname, int port) throws IOException {
        this.hostname = hostname;
        this.port = port;

        sock = new Socket(this.hostname, this.port);
    }

    /**
     * Class for simplifying the handling of RMI messages between hosts
     *
     * @param hostname Hostname to talk to
     * @param port     Port on receiving host
     * @throws java.io.IOException
     */
    public CommHandler(String hostname, String port) throws IOException {
        this.hostname = hostname;
        this.port = Integer.parseInt(port);

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
     * @throws java.io.IOException
     */
    public void sendMessage(Message message) throws IOException {
        // Write the message object to socket's output stream, flushing it to the connected host
        ObjectOutputStream sockOut = new ObjectOutputStream(sock.getOutputStream());
        sockOut.writeObject(message);
        sockOut.flush();
    }

    /**
     * Receive an RMI message from the connected host
     *
     * @return Either the RMI message sent or an Exception message in the event of bad data
     * @throws java.io.IOException
     */
    public Message receiveMessage() throws IOException {
        // Listens to the connected inputstream on the socket, returning the RMI message received
        ObjectInputStream sockIn = new ObjectInputStream(sock.getInputStream());
        try {
            return (Message) sockIn.readObject();
        } catch (ClassNotFoundException e) {
            System.err.println("Error: could not cast returned data as a Message.");
            throw new IOException(e);
        }
    }


    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Overridden equals method (matches on hostname and port)
     */
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        else if (obj == this)
            return true;
        else if (!(obj instanceof CommHandler))
            return false;

        return (((CommHandler) obj).getHostname().equals(hostname) && ((CommHandler) obj).getPort() == port);
    }

    /**
     * Overridden hash method (matches on hostname and port)
     */
    public int hashCode() {
        return 7 * this.port * this.hostname.hashCode();
    }

}
