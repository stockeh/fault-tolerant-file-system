package cs555.system.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import cs555.system.transport.TCPConnection;
import cs555.system.transport.TCPServerThread;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.MinorHeartbeat;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;
import cs555.system.wireformats.RegisterResponse;

/**
 *
 * @author stock
 *
 */
public class Controller implements Node {

  private static final Logger LOG = new Logger();

  private static final String LIST_CHUNK_NODES = "list";

  private static final String HELP = "help";

  private Map<String, TCPConnection> connections = new HashMap<>();

  /**
   * Stands-up the controller as an entry point to the class.
   *
   * @param args
   */
  public static void main(String[] args) {
    if ( args.length < 1 )
    {
      LOG.error( "USAGE: java cs455.overlay.node.Registry portnum" );
      return;
    }

    LOG.info( "Controller starting up at: " + new Date() );
    Controller controller = new Controller();
    try ( ServerSocket serverSocket =
        new ServerSocket( Integer.valueOf( args[ 0 ] ) ) )
    {
      ( new Thread( new TCPServerThread( controller, serverSocket ) ) ).start();

      controller.interact();

    } catch ( IOException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Allow support for commands to be specified while the processes are
   * running.
   */
  private void interact() {
    System.out.println(
        "\nInput a command to interact with processes. Input 'help' for a list of commands.\n" );
    @SuppressWarnings( "resource" )
    Scanner scan = new Scanner( System.in );
    while ( true )
    {
      String line = scan.nextLine().toLowerCase();
      String[] input = line.split( "\\s+" );
      switch ( input[ 0 ] )
      {
        case LIST_CHUNK_NODES :
          displayChunkNodes();
          break;

        case HELP :
          System.out.println( "\n\t" + LIST_CHUNK_NODES
              + "\t\t: show the nodes connected with the controller.\n" );
          break;

        default :
          LOG.error(
              "Unable to process. Please enter a valid command! Input 'help' for options." );
          break;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onEvent(Event event, TCPConnection connection) {
    LOG.debug( event.toString() );
    switch ( event.getType() )
    {
      case Protocol.REGISTER_REQUEST :
        registrationHandler( event, connection, true );
        break;

      case Protocol.DEREGISTER_REQUEST :
        registrationHandler( event, connection, false );
        break;
        
      case Protocol.MINOR_HEARTBEAT :
        heartbeatHandler( event, connection );
        break;

    }
  }

  /**
   * Manage the registry synchronously by either registering a new
   * Client or Chunk Server, or removing one from the system.
   * 
   * @param event the object containing node details
   * @param connection the connection details, i.e., TCPSenderThread
   * @param register true to register new node, false to remove it
   */
  private synchronized void registrationHandler(Event event,
      TCPConnection connection, final boolean register) {
    String nodeDetails = ( ( Register ) event ).getConnection();
    String message = registerStatusMessage( nodeDetails, connection.getSocket()
        .getInetAddress().getHostName().split( "\\." )[ 0 ], register );
    byte status;
    if ( message.length() == 0 )
    {
      if ( register )
      {
        connections.put( nodeDetails, connection );
      } else
      {
        connections.remove( nodeDetails );
        System.out.println( "Deregistered " + nodeDetails + ". There are now ("
            + connections.size() + ") connections.\n" );
      }
      message =
          "Registration request successful.  The number of chunk servers currently "
              + "constituting the network are (" + connections.size() + ").\n";
      status = Protocol.SUCCESS;
    } else
    {
      LOG.error( "Unable to process request. Responding with a failure." );
      status = Protocol.FAILURE;
    }
    LOG.debug( message );
    RegisterResponse response = new RegisterResponse( status, message );
    try
    {
      connection.getTCPSenderThread().sendData( response.getBytes() );
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
      connections.remove( nodeDetails );
    }
  }
  
  private synchronized void heartbeatHandler(Event event, TCPConnection connection) {
    String details = ( ( MinorHeartbeat ) event ).toString();
  }

  /**
   * Verify the node had <b>not</b> previously been registered, and the
   * address that is specified in the registration request and the IP
   * address of the request (the socket’s input stream) match.
   * 
   * @param nodeDetails the host:port from the event message (request)
   * @param connectionIP the remote socket IP address from the current
   *        TCPConnection
   * @return a <code>String</code> containing the error message, or
   *         otherwise empty
   */
  private String registerStatusMessage(String nodeDetails, String connectionIP,
      final boolean register) {
    LOG.debug( "Node Details : " + nodeDetails );
    LOG.debug( "Connection IP: " + connectionIP );
    String message = "";
    if ( connections.containsKey( nodeDetails ) && register )
    {
      message =
          "The node, " + nodeDetails + " had previously registered and has "
              + "a valid entry in its controller. ";
    } else if ( !connections.containsKey( nodeDetails ) && !register )
    { // The case that the item is not in the registry.
      message =
          "The node, " + nodeDetails + " had not previously been registered. ";
    }
    if ( !nodeDetails.split( ":" )[ 0 ].equals( connectionIP )
        && !connectionIP.equals( "localhost" ) )
    {
      message +=
          "There is a mismatch in the address that is specified in request and "
              + "the IP of the socket.";
    }
    return message;
  }

  /**
   * Print out all of the connected Messaging Nodes that have connected.
   * If no nodes are connected, display an error message.
   */
  private void displayChunkNodes() {
    if ( connections.size() == 0 )
    {
      LOG.error(
          "There are no connections in the registry. Initialize a new chunk server." );
    } else
    {
      System.out
          .println( "\nThere are " + connections.size() + " total links:\n" );
      connections.forEach( (k, v) -> System.out.println( "\t" + k ) );
      System.out.println();
    }
  }
}
