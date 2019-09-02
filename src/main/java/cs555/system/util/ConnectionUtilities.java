package cs555.system.util;

import java.io.IOException;
import java.net.Socket;
import cs555.system.node.Node;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;

public class ConnectionUtilities {

  private static final Logger LOG = new Logger();

  /**
   * Registers a node with the controller.
   *
   * @param host identifier for the controller node.
   * @param port number for the controller node
   * @return
   * @throws IOException
   */
  public static TCPConnection registerClient(Node node, String controllerHost,
      Integer controllerPort, String nodeHost, int nodePort)
      throws IOException {
    try
    {
      Socket socketToTheServer = new Socket( controllerHost, controllerPort );
      TCPConnection connection = new TCPConnection( node, socketToTheServer );

      Register register = new Register( Protocol.REGISTER_REQUEST,
          Protocol.CLIENT_ID, nodeHost, nodePort );

      LOG.info( "Client Identifier: " + nodeHost + ":" + nodePort );
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

}
