package cs555.system.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Scanner;
import java.util.Timer;
import cs555.system.transport.TCPConnection;
import cs555.system.transport.TCPServerThread;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.FileUtilities;
import cs555.system.util.HeartbeatHandler;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.RegisterResponse;
import cs555.system.wireformats.WriteChunks;

/**
 * chunk servers initiate and accept both communications and messages
 * within the system.
 *
 * @author stock
 *
 */
public class ChunkServer implements Node, Protocol {

  private static final Logger LOG = new Logger();

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private TCPConnection controllerConnection;

  private String host;

  private int port;

  /**
   * Default constructor - creates a new chunk server tying the
   * <b>host:port</b> combination for the node as the identifier for
   * itself.
   * 
   * @param host
   * @param port
   */
  private ChunkServer(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getHost() {
    return this.host;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPort() {
    return this.port;
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
          "USAGE: java cs555.system.node.ChunkServer controller-host controller-port" );
      System.exit( 1 );
    }
    LOG.info( "chunk server starting up at: " + new Date() );
    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      ChunkServer node =
          new ChunkServer( InetAddress.getLocalHost().getHostName(),
              serverSocket.getLocalPort() );

      ( new Thread( new TCPServerThread( node, serverSocket ),
          "Server Thread" ) ).start();

      node.controllerConnection = ConnectionUtilities.registerNode( node,
          Protocol.CHUNK_ID, args[ 0 ], Integer.valueOf( args[ 1 ] ) );

      HeartbeatHandler heartbeatHandler =
          new HeartbeatHandler( node.controllerConnection );
      Timer timer = new Timer();
      final int interval = 30 * 1000; // 30 seconds in milliseconds
      timer.schedule( heartbeatHandler, 1000, interval );

      node.interact();
    } catch ( IOException e )
    {
      LOG.error( "Unable to successfully start chunk server. Exiting. "
          + e.getMessage() );
      System.exit( 1 );
    }
  }

  /**
   * Allow support for commands to be specified while the processes are
   * running.
   */
  private void interact() {
    System.out.println(
        "\nInput a command to interact with processes. Input 'help' for a list of commands.\n" );
    boolean running = true;
    while ( running )
    {
      @SuppressWarnings( "resource" )
      Scanner scan = new Scanner( System.in );
      switch ( scan.nextLine().toLowerCase() )
      {

        case EXIT :
          ConnectionUtilities.unregisterNode( this, Protocol.CHUNK_ID,
              controllerConnection );
          running = false;
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
    LOG.info( host + ":" + port + " has unregistered and is terminating." );
    System.exit( 0 );
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

      case Protocol.WRITE_CHUNK :
        processIncomingChunk( event );
        break;

      case Protocol.READ_CHUNK :
        break;
    }
  }

  /**
   * 
   * @param event
   */
  private void processIncomingChunk(Event event) {
    WriteChunks request = ( WriteChunks ) event;
    try
    {
      byte[] message = request.getMessage();
      byte[] SHA1Integrity = FileUtilities.SHA1FromBytes( message );
      message = ByteBuffer.allocate( SHA1Integrity.length + message.length )
          .put( SHA1Integrity ).put( message ).array();

      Path path = Paths.get( request.getPath() );
      Files.createDirectories( path.getParent() );
      Files.write( path, message );
    } catch ( NoSuchAlgorithmException e )
    {
      LOG.error( "Unable to compute hash for message. " + e.getMessage() );
      e.printStackTrace();
    } catch ( IOException e )
    {
      LOG.error( "Unable to save chunk " + request.getPath() + " to disk. "
          + e.getMessage() );
      e.printStackTrace();
    }
    // byte[] array = Files.readAllBytes( Paths.get( request.getPath() )
    // );
  }

}
