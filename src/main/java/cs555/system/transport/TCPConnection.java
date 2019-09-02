package cs555.system.transport;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import cs555.system.node.Node;

/**
 * This class is used to establish a connection by starting a new
 * TCPReceiverThread and associating the TCPSender with the socket.
 * 
 * @author stock
 *
 */
public class TCPConnection {

  private Socket socket;

  private TCPSender sender;

  private TCPReceiverThread receiver;

  /**
   * Default constructor - create a new TCPConnection given a Node,
   * i.e., chunk server or controller, and the socket for the connection.
   * 
   * @param node
   * @param socket
   * @throws IOException
   */
  public TCPConnection(Node node, Socket socket) throws IOException {
    this.socket = socket;
    this.sender = new TCPSender( this.socket );
    this.receiver = new TCPReceiverThread( node, this.socket, this );
  }

  /**
   * Get the Socket for the connection to verify INet information
   * 
   * @return the socket for the connection.
   */
  public Socket getSocket() {
    return this.socket;
  }

  /**
   * Get the TCPSender so the client or server can send a message over
   * the socket
   * 
   * @return the TCPSender instance that was instantiated during the
   *         {@link #run()} method of the new thread.
   */
  public TCPSender getTCPSender() {
    return this.sender;
  }

  /**
   * Allow the TCPConnection to start receiving messages.
   * 
   */
  public void start() {
    ( new Thread( this.receiver ) ).start();
  }

  /**
   * Close the socket sender and receiver. Use a one second wait to
   * ensure all remaining messages are sent.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException {
    TimeUnit.SECONDS.sleep( 1 );
    this.socket.close();
    this.sender.dout.close();
    this.receiver.din.close();
  }
}
