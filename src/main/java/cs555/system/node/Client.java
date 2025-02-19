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
import cs555.system.util.Properties;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.ListFileRequest;
import cs555.system.wireformats.ListFileResponse;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.ReadFileRequest;
import cs555.system.wireformats.ReadFileResponse;
import cs555.system.wireformats.RegisterResponse;
import cs555.system.wireformats.WriteFileResponse;

/**
 * Single client to communicate with the file systems controller and
 * chunk servers for writing and reading data.
 * 
 * @author stock
 *
 */
public class Client implements Node {

  public static Logger LOG = Logger.getInstance();

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private static final String UPLOAD = "upload";

  private static final String LIST = "list";

  private static final String READ = "read";

  private final Map<String, ClientReaderThread> readers;

  private ClientSender sender;

  private TCPConnection controllerConnection;

  private final ClientMetadata metadata;

  private final String host;

  private final int port;

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
    LOG.info( "The System is using " + Properties.SYSTEM_DESIGN_SCHEMA
        + " to achieve fault tolerance." );
    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      Client node = new Client( InetAddress.getLocalHost().getHostName(),
          serverSocket.getLocalPort() );

      node.controllerConnection = ConnectionUtilities.registerNode( node,
          Constants.CLIENT_ID, Properties.CONTROLLER_HOST,
          Integer.valueOf( Properties.CONTROLLER_PORT ) );
      node.sender = new ClientSender( node );
      
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
            uploadFiles();
          } catch ( IOException e )
          {
            LOG.error(
                "Failed to read from outbound directory - check arguments. "
                    + e.getMessage() );
            ConnectionUtilities.unregisterNode( this, Constants.CLIENT_ID,
                controllerConnection );
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
            "Invalid argument for \'" + READ + " #\' input.\n" );
      }
      fileNumber = Integer.parseInt( input[ 1 ] );
    } catch ( IllegalArgumentException e )
    {
      LOG.error( "Unable to send read request. " + e.getMessage() );
      return;
    }
    String filename = null;
    try
    {
      filename = metadata.getReadableFiles().get( fileNumber );
    } catch ( IndexOutOfBoundsException e )
    {
      displayReadableFiles();
      return;
    }
    if ( filename != null )
    {
      try
      {
        controllerConnection.getTCPSender()
            .sendData( new ReadFileRequest( filename ).getBytes() );
      } catch ( IOException e )
      {
        LOG.error(
            "Unable to send read request to controller. " + e.getMessage() );
        e.printStackTrace();
      }
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
        Files.walk( Paths.get( Properties.CLIENT_OUTBOUND_DIRECTORY ) ) )
    {
      files = paths.filter( Files::isRegularFile ).map( Path::toFile )
          .collect( Collectors.toList() );
    }
    if ( files == null || files.isEmpty() )
    {
      LOG.info( "There are no files to upload in "
          + Properties.CLIENT_OUTBOUND_DIRECTORY );
    } else
    {
      sender.send( files );
    }
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
      case Protocol.REGISTER_RESPONSE :
        System.out.println( ( ( RegisterResponse ) event ).toString() );
        break;

      case Protocol.WRITE_FILE_RESPONSE :
        senderHandler( event );
        break;

      case Protocol.LIST_FILE_RESPONSE :
        metadata
            .setReadableFiles( ( ( ListFileResponse ) event ).getFileNames() );
        displayReadableFiles();
        break;

      case Protocol.READ_FILE_RESPONSE :
        readFileResponseHandler( event );
        break;

      case Protocol.READ_CHUNK_RESPONSE :
        readChunkResponseHandler( event );
        break;

      case Protocol.FAILURE_CLIENT_NOTIFICATION :
        metadata.clearReadableFiles();
        break;
    }
  }

  /**
   * Process an incoming chunk from a given chunk server.
   * 
   * @param event the object containing message details
   */
  private void readChunkResponseHandler(Event event) {
    ReadChunkResponse response = ( ReadChunkResponse ) event;
    ClientReaderThread reader = readers.get( response.getFilename() );
    if ( reader == null )
    {
      LOG.error( "Unable to retrieve reader thread to obtain file." );
      return;
    }
    reader.setReadChunkResponse( response );
    reader.unlock();
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
    ClientReaderThread reader =
        new ClientReaderThread( this, metadata, response );
    readers.put( response.getFilename(), reader );
    LOG.debug( "Starting client reader thread." );
    ( new Thread( reader, "Client Reader" ) ).start();
  }

  /**
   * Display the readable files in a readable way.
   * 
   */
  private void displayReadableFiles() {
    List<String> readableFiles = metadata.getReadableFiles();
    if ( readableFiles.size() == 0 )
    {
      System.out.println( "\nThere are no readable files known to the client."
          + "\n\nEither (1) upload files with the \'" + UPLOAD
          + "\' input, or (2) fetch a list from the controller with the \'"
          + LIST + "\' input.\n" );
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
   * @param event the object containing message details
   */
  private void senderHandler(Event event) {
    WriteFileResponse response = ( WriteFileResponse ) event;
    sender.setAbleToWrite( response.isAbleToWrite() );
    sender.setRoutes( response );
  }

  /**
   * Display a help message for how to interact with the application.
   * 
   */
  private void displayHelp() {
    System.out.println( "\n\t" + EXIT
        + "\t: disconnect from the controller and terminate.\n\n\t" + UPLOAD
        + "\t: upload all files in " + Properties.CLIENT_OUTBOUND_DIRECTORY
        + "\n\n\t" + LIST
        + "\t: list readable files stored on the chunk servers." + "\n\n\t"
        + READ + " #\t: read a file identified by a number listed from the \'"
        + LIST + "\' input.\n" );
  }

}
