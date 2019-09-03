package cs555.system.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ClientSenderThread;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.Protocol;

/**
 * Single client to communicate with the file systems controller and
 * chunk servers for writing and reading data.
 * 
 * @author stock
 *
 */
public class Client implements Node {

  public static Logger LOG = new Logger();

  private final Object senderLock = new Object();

  private Thread senderThread = null;

  private TCPConnection controllerConnection;

  private String outboundDirectory;

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private static final String UPLOAD = "upload";

  private static final String LIST_FILES = "list";

  private String host;

  private int port;

  /**
   * Default constructor - creates a new client tying the
   * <b>host:port</b> combination for the node as the identifier for
   * itself.
   * 
   * @param host
   * @param port
   */
  private Client(String host, int port) {
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
   * Initialize the client with the Controller.
   *
   * @param args
   */
  public static void main(String[] args) {
    if ( args.length < 3 )
    {
      LOG.error(
          "USAGE: java cs555.system.node.Client controller-host controller-port outbound-dir" );
      System.exit( 1 );
    }
    LOG.info( "Client Node starting up at: " + new Date() );

    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      Client node = new Client( InetAddress.getLocalHost().getHostName(),
          serverSocket.getLocalPort() );

      node.controllerConnection = ConnectionUtilities.registerNode( node,
          Protocol.CLIENT_ID, args[ 0 ], Integer.valueOf( args[ 1 ] ) );

      node.outboundDirectory = args[ 2 ];
      node.interact();
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to successfully start client. Exiting. " + e.getMessage() );
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
        case UPLOAD :
          try
          {
            if ( senderThread == null || !senderThread.isAlive() )
            {
              uploadFiles();
            } else
            {
              LOG.info(
                  "Files are currently being uploaded. Await completion before restarting." );
            }
          } catch ( IOException e )
          {
            LOG.error(
                "Failed to read from outbound directory - check arguments. "
                    + e.getMessage() );
            ConnectionUtilities.unregisterNode( this, Protocol.CLIENT_ID,
                controllerConnection );
            running = false;
          }
          break;

        case LIST_FILES :
          listControllerFiles();
          break;

        case EXIT :
          ConnectionUtilities.unregisterNode( this, Protocol.CLIENT_ID,
              controllerConnection );
          running = false;
          break;

        case HELP :
          displayHelp();
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
   * Grab outbound files to send to the controller. Return if there are
   * no files to upload.
   * 
   * @throws IOException if there are issues reading the outbound
   *         directory.
   */
  private void uploadFiles() throws IOException {
    List<File> files;
    try ( Stream<Path> paths = Files.walk( Paths.get( outboundDirectory ) ) )
    {
      files = paths.filter( Files::isRegularFile ).map( Path::toFile )
          .collect( Collectors.toList() );
    }
    if ( files == null || files.isEmpty() )
    {
      LOG.info( "There are no files to upload in " + outboundDirectory );
      return;
    }
    ( senderThread = new Thread( new ClientSenderThread( files, this.senderLock,
        this.controllerConnection ) ) ).start();
  }

  /**
   * Send a request to the controller to display all files that are
   * stored on the chunk servers.
   * 
   */
  private void listControllerFiles() {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onEvent(Event event, TCPConnection connection) {
    LOG.debug( event.toString() );
    switch ( event.getType() )
    {
      case Protocol.WRITE_QUERY_RESPONSE :
        synchronized ( this.senderLock )
        {
          this.senderLock.notify();
        }
        break;
    }
  }

  /**
   * Display a help message for how to interact with the application.
   * 
   */
  private void displayHelp() {
    System.out.println( "\n\t" + EXIT
        + "\t: disconnect from the controller and terminate.\n\n\t" + UPLOAD
        + "\t: upload all files in " + outboundDirectory + "\n\n\t" + LIST_FILES
        + "\t: list readable files stored on the chunk servers.\n" );
  }

}
