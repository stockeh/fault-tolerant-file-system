package cs555.system.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import cs555.system.metadata.ClientMetadata;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.ListFileRequest;
import cs555.system.wireformats.ListFileResponse;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.ReadFileRequest;
import cs555.system.wireformats.ReadFileResponse;
import cs555.system.wireformats.WriteFileResponse;

/**
 * Single client to communicate with the file systems controller and
 * chunk servers for writing and reading data.
 * 
 * @author stock
 *
 */
public class Client implements Node {

  public static Logger LOG = new Logger();

  /**
   * <k: filename, v: Thread()>
   */
  private Map<String, ClientReaderThread> readers;

  private ClientSenderThread sender = null;

  private TCPConnection controllerConnection;

  private ClientMetadata metadata;

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private static final String UPLOAD = "upload";

  private static final String LIST = "list";

  private static final String READ = "read";

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
    this.readers = new HashMap<>();
    this.metadata = new ClientMetadata();
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
   * 
   * @return the connection to the controller
   */
  public TCPConnection getControllerConnection() {
    return this.controllerConnection;
  }

  /**
   * Initialize the client with the Controller.
   *
   * @param args
   */
  public static void main(String[] args) {
    LOG.info( "Client node starting up at: " + new Date() );

    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      Client node = new Client( InetAddress.getLocalHost().getHostName(),
          serverSocket.getLocalPort() );

      node.controllerConnection = ConnectionUtilities.registerNode( node,
          Constants.CLIENT_ID, Constants.CONTROLLER_HOST,
          Integer.valueOf( Constants.CONTROLLER_PORT ) );

      node.sender = new ClientSenderThread( node );
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
      String[] input = scan.nextLine().toLowerCase().split( "\\s+" );
      switch ( input[ 0 ] )
      {
        case UPLOAD :
          try
          {
            if ( sender == null || !sender.isRunning() )
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
            ConnectionUtilities.unregisterNode( this, Constants.CLIENT_ID,
                controllerConnection );
            running = false;
          }
          break;

        case LIST :
          listFilesRequest();
          break;

        case READ :
          readFileRequest( input );
          break;

        case EXIT :
          ConnectionUtilities.unregisterNode( this, Constants.CLIENT_ID,
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
   * Request to the controller to return a list of chunk servers to read
   * a given file.
   * 
   * @param input from the user scanner, e.g., 'read 2'
   */
  private void readFileRequest(String[] input) {
    int fileNumber = -1;
    try
    {
      if ( input.length != 2 )
      {
        throw new IllegalArgumentException(
            "Invalid argument for \'" + READ + "\' input." );
      }
      fileNumber = Integer.parseInt( input[ 1 ] );
    } catch ( IllegalArgumentException e )
    {
      LOG.error( "Unable to send read request. " + e.getMessage() );
      e.printStackTrace();
    }
    try
    {
      ReadFileRequest request =
          new ReadFileRequest( metadata.getReadableFiles().get( fileNumber ) );
      controllerConnection.getTCPSender().sendData( request.getBytes() );
    } catch ( IOException | IndexOutOfBoundsException e )
    {
      LOG.error(
          "Unable to send read request to controller. " + e.getMessage() );
      e.printStackTrace();
    }

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
    try ( Stream<Path> paths =
        Files.walk( Paths.get( Constants.CLIENT_OUTBOUND_DIRECTORY ) ) )
    {
      files = paths.filter( Files::isRegularFile ).map( Path::toFile )
          .collect( Collectors.toList() );
    }
    if ( files == null || files.isEmpty() )
    {
      LOG.info( "There are no files to upload in "
          + Constants.CLIENT_OUTBOUND_DIRECTORY );
      return;
    }
    sender.setFiles( files );
    ( new Thread( sender ) ).start();
  }

  /**
   * Send a request to the controller to display all files that are
   * stored on the chunk servers.
   * 
   */
  private void listFilesRequest() {
    try
    {
      controllerConnection.getTCPSender()
          .sendData( new ListFileRequest().getBytes() );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send request message to controller. " + e.getMessage() );
      e.printStackTrace();
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
      case Protocol.WRITE_FILE_RESPONSE :
        senderHandler( event );
        break;

      case Protocol.LIST_FILE_RESPONSE :
        displayReadableFiles( event );
        break;

      case Protocol.READ_FILE_RESPONSE :
        readFileResponseHandler( event );
        break;

      case Protocol.READ_CHUNK_RESPONSE :
        readChunkResponseHandler( event );
    }
  }

  private void readChunkResponseHandler(Event event) {
    ReadChunkResponse response = ( ReadChunkResponse ) event;
    ClientReaderThread reader = readers.get( response.getFilename() );
    
    if ( reader == null )
    {
      LOG.error( "Unable to retrieve reader thread to obtain file." );
      return;
    }
    
  }

  /**
   * A response from the controller containing chunk locations for a
   * given file will trigger this handler to start requesting individual
   * chunk servers for chunks of said file.
   * 
   * @param event the object containing message details
   */
  private void readFileResponseHandler(Event event) {
    ReadFileResponse response = ( ( ReadFileResponse ) event );
    ClientReaderThread reader = new ClientReaderThread( this, response );
    readers.put( response.getFilename(), reader );
    ( new Thread( reader ) ).start();
  }

  /**
   * Process the response from the controller to list the files in a
   * readable way.
   * 
   * @param event
   */
  private void displayReadableFiles(Event event) {
    List<String> readableFiles = ( ( ListFileResponse ) event ).getFileNames();
    metadata.setReadableFiles( readableFiles );
    if ( readableFiles.size() == 0 )
    {
      System.out
          .println( "\nThere are no readable files known to the controller."
              + "\nPlease upload files with the \'" + UPLOAD + "\' input.\n" );
      return;
    }
    System.out.println( "\tThere are " + readableFiles.size()
        + " file(s) available to read.\n" );
    for ( int i = 0; i < readableFiles.size(); ++i )
    {
      System.out.println(
          "\t" + Integer.toString( i ) + "\t: " + readableFiles.get( i ) );
    }
    System.out.println( "\nRead a file using the \'" + READ
        + " #\' input with the associated number.\n" );
  }

  /**
   * Routes have been received from the controller, so the client sender
   * can be unlocked.
   * 
   * @param event
   */
  private void senderHandler(Event event) {
    String[] routes = ( ( WriteFileResponse ) event ).getRoutingPath();
    sender.setRoutes( routes );
    sender.unlock();
  }

  /**
   * Display a help message for how to interact with the application.
   * 
   */
  private void displayHelp() {
    System.out.println( "\n\t" + EXIT
        + "\t: disconnect from the controller and terminate.\n\n\t" + UPLOAD
        + "\t: upload all files in " + Constants.CLIENT_OUTBOUND_DIRECTORY
        + "\n\n\t" + LIST
        + "\t: list readable files stored on the chunk servers." + "\n\n\t"
        + READ + " #\t: read a file identified by a number listed from the \'"
        + LIST + "\' input.\n" );
  }

}
