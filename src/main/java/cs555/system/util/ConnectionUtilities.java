package cs555.system.util;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import cs555.system.node.Node;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.RegisterRequest;

/**
 * Shared connection utilities between the controller and client /
 * chunk servers.
 * 
 * @author stock
 *
 */
public class ConnectionUtilities {

  private static final Logger LOG = Logger.getInstance();

  private final Map<String, TCPConnection> temporaryConnections;

  private final StringBuilder connectionStringBuilder;

  private boolean ableToClear;

  /**
   * Default constructor -
   * 
   */
  public ConnectionUtilities() {
    this.temporaryConnections = new HashMap<>();
    this.connectionStringBuilder = new StringBuilder();
    this.ableToClear = false;
  }

  /**
   * Allow the connections to be closed 
   * 
   * @param ableToClear
   */
  public synchronized void setAbleToClear(boolean ableToClear) {
    this.ableToClear = ableToClear;
  }

  /**
   * Either establish a new or retrieve a cached connection made
   * previously.
   * 
   * @param startConnection true to start the TCP Receiver Thread, false
   *        otherwise
   * @param connectionDetails to connect to
   * @return the cached TCP connection
   * @throws IOException
   * @throws NumberFormatException
   */
  public synchronized TCPConnection cacheConnection(Node node,
      String[] initialConnection, boolean startConnection)
      throws NumberFormatException, IOException {
    ableToClear = false;
    String connectionDetails =
        connectionStringBuilder.append( initialConnection[ 0 ] ).append( ":" )
            .append( initialConnection[ 1 ] ).toString();
    connectionStringBuilder.setLength( 0 );

    TCPConnection connection;
    if ( temporaryConnections.containsKey( connectionDetails ) )
    {
      connection = temporaryConnections.get( connectionDetails );
    } else
    {
      connection = ConnectionUtilities.establishConnection( node,
          initialConnection[ 0 ], Integer.parseInt( initialConnection[ 1 ] ) );
      temporaryConnections.put( connectionDetails, connection );
      if ( startConnection )
      {
        connection.start();
      }
    }
    return connection;
  }

  /**
   * Close and remove all temporary connections.
   * 
   */
  public synchronized void closeCachedConnections() {
    if ( ableToClear )
    {
      temporaryConnections.forEach( (k, v) ->
      {
        try
        {
          v.close();
        } catch ( IOException | InterruptedException e )
        {
          LOG.error( "Unable to close the connection for " + k );
        }
      } );
      temporaryConnections.clear();
    }
  }

  /**
   * Establish generic connection with a given node.
   * 
   * @param node used to controller receiving thread
   * @param host name associated with outgoing node
   * @param port number associated with outgoing node
   * @return connection to server
   * @throws IOException
   */
  public static TCPConnection establishConnection(Node node, String host,
      Integer port) throws IOException {
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

      RegisterRequest registerRequest =
          new RegisterRequest( Protocol.REGISTER_REQUEST, identifier,
              node.getHost(), node.getPort() );

      LOG.info( "Client Identifier: " + node.getHost() + ":" + node.getPort() );
      connection.getTCPSender().sendData( registerRequest.getBytes() );
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
    RegisterRequest registerRequest =
        new RegisterRequest( Protocol.UNREGISTER_REQUEST, identifier,
            node.getHost(), node.getPort() );

    try
    {
      controllerConnection.getTCPSender()
          .sendData( registerRequest.getBytes() );
      controllerConnection.close();
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

}
