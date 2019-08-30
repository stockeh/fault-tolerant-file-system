package cs555.system.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import cs555.system.transport.TCPConnection;
import cs555.system.transport.TCPServerThread;
import cs555.system.util.HeartbeatHandler;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;
import cs555.system.wireformats.RegisterResponse;

/**
 * Messaging nodes initiate and accept both communications and
 * messages within the system.
 *
 * @author stock
 *
 */
public class ChunkServer implements Node, Protocol {

  private static final Logger LOG = new Logger();

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private TCPConnection controllerConnection;

  private Map<String, TCPConnection> connections = new ConcurrentHashMap<>();

  private Integer nodePort;

  private String nodeHost;

  /**
   * Default constructor - creates a new chunk server tying the
   * <b>host:port</b> combination for the node as the identifier for
   * itself.
   * 
   * @param nodeHost
   * @param nodePort
   */
  private ChunkServer(String nodeHost, int nodePort) {
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
  }

  /**
   * Start up a new TCPServerThread for the chuck server to listen on
   * then register the node with the controller.
   *
   * @param args
   */
  public static void main(String[] args) {
    if ( args.length < 2 )
    {
      LOG.error(
          "USAGE: java cs555.system.node.ChunkServer registry-host registry-port" );
      System.exit( 1 );
    }
    LOG.info( "Messaging Node starting up at: " + new Date() );
    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      int nodePort = serverSocket.getLocalPort();
      ChunkServer node =
          new ChunkServer( InetAddress.getLocalHost().getHostName(), nodePort );
      ( new Thread( new TCPServerThread( node, serverSocket ) ) ).start();
      node.registerNode( args[ 0 ], Integer.valueOf( args[ 1 ] ) );
      
      HeartbeatHandler heartbeatHandler = new HeartbeatHandler(node.controllerConnection);
      Timer timer = new Timer();
      final int interval = 30 * 1000; // 30 seconds in milliseconds
      timer.schedule( heartbeatHandler, 1000, interval );
      
      node.interact();
    } catch ( IOException e )
    {
      LOG.error( "Exiting " + e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Registers a node with the registry.
   *
   * @param host identifier for the registry node.
   * @param port number for the registry node
   */
  private void registerNode(String registryHost, Integer registryPort) {
    try
    {
      Socket socketToTheServer = new Socket( registryHost, registryPort );
      TCPConnection connection = new TCPConnection( this, socketToTheServer );

      Register register = new Register( Protocol.REGISTER_REQUEST,
          this.nodeHost, this.nodePort );

      LOG.info(
          "MessagingNode Identifier: " + this.nodeHost + ":" + this.nodePort );
      connection.getTCPSender().sendData( register.getBytes() );
      connection.start();

      this.controllerConnection = connection;
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
  @SuppressWarnings( "resource" )
  private void interact() {
    System.out.println(
        "\nInput a command to interact with processes. Input 'help' for a list of commands.\n" );
    boolean running = true;
    while ( running )
    {
      Scanner scan = new Scanner( System.in );
      switch ( scan.nextLine().toLowerCase() )
      {

        case EXIT :
          if ( connections.size() == 0 )
          {
            exitOverlay();
            running = false;
          } else
          {
            LOG.error(
                "Chunk Server is connected with the client. Unable to leave the overlay.\n" );
          }
          break;

        case HELP :
          System.out.println(
              "\n\t" + EXIT + "\t: leave the overlay prior to starting.\n" );
          break;

        default :
          LOG.error(
              "Unable to process. Please enter a valid command! Input 'help' for options." );
          break;
      }
    }
    LOG.info(
        nodeHost + ":" + nodePort + " has deregistered and is terminating." );
    System.exit( 0 );
  }

  /**
   * Remove the node from the registry. This must occur prior to setting
   * up the overlay on the registry.
   * 
   * TODO: Do I close the socket here? Current exceptions.
   */
  private void exitOverlay() {
    Register register = new Register( Protocol.DEREGISTER_REQUEST,
        this.nodeHost, this.nodePort );

    try
    {
      controllerConnection.getTCPSender().sendData( register.getBytes() );
      controllerConnection.close();
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
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
      case Protocol.REGISTER_RESPONSE :
        System.out.println( ( ( RegisterResponse ) event ).toString() );
        break;

      case Protocol.REGISTER_REQUEST :
        acknowledgeNewConnection( event, connection );
        break;
    }
  }



  /**
   * Acknowledge "incoming" connections and add connection to
   * this.connections. Allows for this to send bidirectional message.
   * 
   * @param event
   * @param connection
   */
  private void acknowledgeNewConnection(Event event, TCPConnection connection) {
    String nodeDetails = ( ( Register ) event ).getConnection();
    connections.put( nodeDetails, connection );
  }
}
