package cs555.system.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Timer;
import cs555.system.heartbeat.ControllerHeartbeatManager;
import cs555.system.metadata.ControllerMetadata;
import cs555.system.metadata.ControllerMetadata.FileInformation;
import cs555.system.metadata.ServerMetadata.ChunkInformation;
import cs555.system.transport.TCPConnection;
import cs555.system.transport.TCPServerThread;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.FailureChunkRead;
import cs555.system.wireformats.Heartbeat;
import cs555.system.wireformats.ListFileResponse;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.ReadFileRequest;
import cs555.system.wireformats.ReadFileResponse;
import cs555.system.wireformats.RedirectChunkRequest;
import cs555.system.wireformats.RegisterRequest;
import cs555.system.wireformats.RegisterResponse;
import cs555.system.wireformats.WriteFileRequest;
import cs555.system.wireformats.WriteFileResponse;

/**
 * The control flow of the application occurs through the controller
 * managing the chunk servers.
 * 
 * Chunk servers and clients alike will establish connections with the
 * controller.
 *
 * @author stock
 *
 */
public class Controller implements Node {

  private static final Logger LOG = new Logger();

  private static final String LIST_CHUNK_NODES = "list";

  private static final String HELP = "help";

  private ControllerMetadata metadata;

  private String host;

  private int port;

  /**
   * Default constructor - creates a new controller tying the
   * <b>host:port</b> combination for the node as the identifier for
   * itself.
   * 
   * @param host
   * @param port
   */
  public Controller(String host, int port) {
    this.metadata = new ControllerMetadata();
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
   * Stands-up the controller as an entry point to the class.
   *
   * @param args
   */
  public static void main(String[] args) {
    LOG.info( "Controller node starting up at: " + new Date() );

    try ( ServerSocket serverSocket =
        new ServerSocket( Integer.valueOf( Constants.CONTROLLER_PORT ) ) )
    {
      Controller controller =
          new Controller( InetAddress.getLocalHost().getHostName(),
              serverSocket.getLocalPort() );

      ( new Thread( new TCPServerThread( controller, serverSocket ),
          "Server Thread" ) ).start();

      if ( Constants.SYSTEM_DESIGN_SCHEMA
          .equals( Constants.SYSTEM_TYPE_REPLICATION ) )
      {
        ControllerHeartbeatManager controllerHeartbeatManager =
            new ControllerHeartbeatManager( controller.metadata );
        Timer timer = new Timer();
        final int interval = 20 * 1000; // 20 seconds in milliseconds
        timer.schedule( controllerHeartbeatManager, 1000, interval );
      }
      controller.interact();
    } catch ( IOException e )
    {
      LOG.error( "Unable to successfully start controller. Exiting. "
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
    @SuppressWarnings( "resource" )
    Scanner scan = new Scanner( System.in );
    while ( true )
    {
      String line = scan.nextLine().toLowerCase();
      String[] input = line.split( "\\s+" );
      switch ( input[ 0 ] )
      {
        case LIST_CHUNK_NODES :
          metadata.displayConnections();
          break;

        case HELP :
          System.out.println( "\n\t" + LIST_CHUNK_NODES
              + "\t: show the nodes connected with the controller.\n" );
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

      case Protocol.UNREGISTER_REQUEST :
        registrationHandler( event, connection, false );
        break;

      case Protocol.MINOR_HEARTBEAT :
        minorHeartbeatHandler( event );
        break;

      case Protocol.MAJOR_HEARTBEAT :
        minorHeartbeatHandler( event );
        majorHeartbeatHandler( event );
        break;

      case Protocol.WRITE_FILE_REQUEST :
        writeFileRequestHandler( event, connection );
        break;

      case Protocol.LIST_FILE_REQUEST :
        listFileRequestHandler( connection );
        break;

      case Protocol.READ_FILE_REQUEST :
        readFileRequestHandler( event, connection );
        break;

      case Protocol.FAILURE_CHUNK_READ :
        failureChunkReadHandler( event );
        break;
    }
  }

  /**
   * This handler is triggered when the chunk server detects a failure
   * for a given read request. A failure message is sent to the
   * controller to try and reconcile the server by sending a copy for
   * some chunk from a source to the destination server.
   * 
   * @param event
   */
  private void failureChunkReadHandler(Event event) {
    FailureChunkRead request = ( FailureChunkRead ) event;
    String destination = request.getConnectionDetails();

    FileInformation info = metadata.getFiles().get( request.getFilename() );
    String[][] chunks = info.getChunks();

    int sequence = request.getSequence();
    int replicationPosition = 0;
    String source = null;
    for ( int replication = 0; replication < chunks[ 0 ].length; ++replication )
    {
      String identifier = chunks[ sequence ][ replication ];
      if ( identifier != null && !identifier.equals( destination ) )
      {
        source = identifier;
      }
      if ( identifier != null && identifier.equals( destination ) )
      {
        chunks[ sequence ][ replication ] = null;
        replicationPosition = replication;
      }
    }
    if ( source != null )
    {
      RedirectChunkRequest redirectRequest = new RedirectChunkRequest(
          request.getFilename(), sequence, replicationPosition, destination );
      try
      {
        LOG.debug( "Sending RedirectChunkRequest from: " + source + " to "
            + destination + " for sequence " + sequence + " & replication "
            + replicationPosition );
        metadata.getConnections().get( source ).getConnection().getTCPSender()
            .sendData( redirectRequest.getBytes() );
      } catch ( IOException e )
      {
        LOG.error( "Unable to send request to server \'" + source
            + "\' to update the destination \'" + destination + "\'. "
            + e.getMessage() );
        e.printStackTrace();
      }
    } else
    {
      LOG.error(
          "A source containing the replication for the chunk could not be"
              + "identified." );
      return;
    }
  }

  /**
   * Construct a message to send back to the client containing
   * information of where to retrieve chunk data.
   * 
   * @param event the object containing node details
   * @param connection the connection details, i.e., TCPSender
   */
  private void readFileRequestHandler(Event event, TCPConnection connection) {
    String filename = ( ( ReadFileRequest ) event ).getFilename();
    FileInformation fileInformation = metadata.getFiles().get( filename );
    ReadFileResponse response = new ReadFileResponse( filename,
        fileInformation.getFilelength(), fileInformation.getChunks() );
    try
    {
      connection.getTCPSender().sendData( response.getBytes() );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send response message to client. " + e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Upon the client sending a request for the files available to read,
   * the controller will respond with a name of readable filenames.
   * 
   * @param connection
   */
  private void listFileRequestHandler(TCPConnection connection) {
    ListFileResponse response =
        new ListFileResponse( metadata.getReadableFiles() );
    try
    {
      connection.getTCPSender().sendData( response.getBytes() );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send response message to client. " + e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Construct a message to send back to the client containing
   * information of where to send chuck data too.
   * 
   * @param event the object containing node details
   * @param connection the connection details, i.e., TCPSender
   */
  private void writeFileRequestHandler(Event event, TCPConnection connection) {
    WriteFileRequest request = ( WriteFileRequest ) event;

    boolean isOriginalFile = metadata.addFile( request.getFilename(),
        request.getFilelength(), request.getNumberOfChunks() );
    String[] serversToConnect = metadata.getChunkServers( request.getFilename(),
        request.getSequence(), isOriginalFile );
    // All heartbeats been received for sequence 0.
    WriteFileResponse response = new WriteFileResponse( serversToConnect );
    try
    {
      connection.getTCPSender().sendData( response.getBytes() );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send response message to client. " + e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Manage the controller synchronously by either registering a new
   * Client or Chunk Server, or removing one from the system.
   * 
   * @param event the object containing node details
   * @param connection the connection details, i.e., TCPSender
   * @param register true to register new node, false to remove it
   */
  private synchronized void registrationHandler(Event event,
      TCPConnection connection, final boolean register) {
    RegisterRequest request = ( RegisterRequest ) event;
    String connectionDetails = request.getConnection();
    int identifier = request.getIdentifier();
    String message =
        registerStatusMessage( connectionDetails, connection.getSocket()
            .getInetAddress().getHostName().split( "\\." )[ 0 ], register );
    byte status = 0;
    if ( message.length() == 0 )
    {
      if ( register )
      {
        switch ( identifier )
        {
          case Constants.SERVER_ID :
            metadata.addConnection( connectionDetails, connection );
            status = Constants.SUCCESS;
            break;
          case Constants.CLIENT_ID :
            metadata.addClientConnection( connection );
            status = Constants.SUCCESS;
            break;
        }
      } else
      {
        switch ( identifier )
        {
          case Constants.SERVER_ID :
            // TODO: replicate files if needed.
            metadata.removeConnection( connectionDetails );
            status = Constants.SUCCESS;
            break;
          case Constants.CLIENT_ID :
            metadata.removeClientConnection( connection );
            status = Constants.SUCCESS;
            break;
        }
      }
      if ( status == Constants.SUCCESS )
      {
        message =
            "Registration request successful.  The number of chunk servers currently "
                + "constituting the network are ("
                + metadata.numberOfConnections() + ").\n";
      }
    }
    LOG.info( message );
    RegisterResponse response = new RegisterResponse( status, message );
    try
    {
      connection.getTCPSender().sendData( response.getBytes() );
    } catch ( IOException e )
    {
      LOG.error( e.getMessage() );
      metadata.removeConnection( connectionDetails );
    }
  }

  /**
   * Verify the node had <b>not</b> previously been registered, and the
   * address that is specified in the registration request and the IP
   * address of the request (the socket’s input stream) match.
   * 
   * @param connectionDetails the host:port from the event message
   *        (request)
   * @param connectionIP the remote socket IP address from the current
   *        TCPConnection
   * @return a <code>String</code> containing the error message, or
   *         otherwise empty
   */
  private String registerStatusMessage(String connectionDetails,
      String connectionIP, final boolean register) {
    LOG.debug( "Connection Details : " + connectionDetails );
    LOG.debug( "Connection IP: " + connectionIP );
    String message = "";
    if ( metadata.connectionsContainsKey( connectionDetails ) && register )
    {
      message = "The node, " + connectionDetails
          + " had previously registered and has "
          + "a valid entry in its controller. ";
    } else if ( !metadata.connectionsContainsKey( connectionDetails )
        && !register )
    { // The case that the chunk server is not already connected to the
      // controller.
      message = "The node, " + connectionDetails
          + " had not previously been registered. ";
    }
    if ( !connectionDetails.split( ":" )[ 0 ].equals( connectionIP )
        && !connectionIP.equals( "localhost" ) )
    {
      message +=
          "There is a mismatch in the address that is specified in request and "
              + "the IP of the socket.";
    }
    return message;
  }

  /**
   * Manage the incoming <b>MINOR</b> heartbeats by updating the
   * controller metadata
   * 
   * @param event
   */
  private synchronized void minorHeartbeatHandler(Event event) {
    Heartbeat request = ( ( Heartbeat ) event );
    try
    {
      // TODO: Should the total chunks be written here from the server, or
      // just be incremented from the controller.
      metadata.updateServerInformation( request.getConnectionDetails(),
          request.getFreeSpace() );
      if ( !request.isEmpty() )
      {
        metadata.updateFileInformation( request.getFiles(),
            request.getConnectionDetails() );
      }
    } catch ( NullPointerException e )
    {
      LOG.error( e.getMessage() );
    }
  }

  /**
   * Manage the incoming <b>MAJOR</b> heartbeats by updating the
   * controller metadata
   * 
   * @param event
   */
  private synchronized void majorHeartbeatHandler(Event event) {
    Heartbeat request = ( ( Heartbeat ) event );

    String serversize = new DecimalFormat( "0.00000000" ).format(
        ( ( ( request.getFreeSpace() / 1024.0 ) / 1024.0 ) / 1024.0 ) );
    String lineSeparator = new String( new char[ 90 ] ).replace( "\0", "-" );

    System.out.println( "\n" + lineSeparator );

    System.out.format( "%30s%20s%15s\n",
        new Object[] { request.getConnectionDetails(), serversize + " (GB)",
            request.getTotalChunks() + " chunk(s)" } );

    if ( request.isEmpty() )
    {
      System.out.println( "\nThere is no additional information to display." );
      System.out.println( "\n" + lineSeparator );
      return;
    }

    System.out.format( "%30s%20s%15s%25s\n", new Object[] { "Filename",
        "Sequence", "Version", "Modification Date" } );

    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );

    for ( Entry<String, List<ChunkInformation>> entry : request.getFiles()
        .entrySet() )
    {
      String title = entry.getKey();
      title = title.substring( title.lastIndexOf( File.separator ) );
      int i = 0;
      for ( ChunkInformation info : entry.getValue() )
      {
        title = i++ == 0 ? title : "";
        System.out.format( "%30s%20s%15s%25s\n",
            new Object[] { title, info.getSequence(), info.getVersion(),
                sdf.format( info.getLastModifiedTime() ) } );
      }
    }
    System.out.println( lineSeparator );
  }
}
