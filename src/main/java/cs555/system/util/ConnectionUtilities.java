package cs555.system.util;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import cs555.system.node.Node;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;

/**
 * Shared connection utilities between the controller and client /
 * chunk servers.
 * 
 * @author stock
 *
 */
public class ConnectionUtilities {

  private static final Logger LOG = new Logger();

  /**
   * Establish generic connection with a given node.
   * 
   * @param node used to controller receiving thread
   * @param host name associated with outgoing node
   * @param port number associated with outgoing node
   * @return connection to server
   * @throws UnknownHostException
   * @throws IOException
   */
  public static TCPConnection establishConnection(Node node, String host,
      Integer port) throws UnknownHostException, IOException {
    Socket socketToTheServer = new Socket( host, port );
    return new TCPConnection( node, socketToTheServer );
  }

  /**
   * Registers a node with the controller.
   *
   * @param node requesting to connect
   * @param identifier distinguishes the type of node
   * @param controllerHost identifier for the controller node
   * @param controllerPort number for the controller node
   * 
   * @return a TCPConnection to the controller
   * @throws IOException
   */
  public static TCPConnection registerNode(Node node, int identifier,
      String controllerHost, Integer controllerPort) throws IOException {
    try
    {
      TCPConnection connection =
          establishConnection( node, controllerHost, controllerPort );

      Register register = new Register( Protocol.REGISTER_REQUEST, identifier,
          node.getHost(), node.getPort() );

      LOG.info( "Client Identifier: " + node.getHost() + ":" + node.getPort() );
      connection.getTCPSender().sendData( register.getBytes() );
      connection.start();

      return connection;
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to connect to the controller. Check that it is running, and the connection details are correct. "
              + e.getMessage() );
      throw e;
    }
  }

  /**
   * Unregister a node with the controller.
   * 
   * @param node
   * @param identifier
   * @param controllerConnection
   */
  public static void unregisterNode(Node node, int identifier,
      TCPConnection controllerConnection) {
    Register register = new Register( Protocol.UNREGISTER_REQUEST, identifier,
        node.getHost(), node.getPort() );

    try
    {
      controllerConnection.getTCPSender().sendData( register.getBytes() );
      controllerConnection.close();
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

}
