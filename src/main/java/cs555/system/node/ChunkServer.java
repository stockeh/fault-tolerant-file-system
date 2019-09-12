package cs555.system.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Scanner;
import java.util.Timer;
import cs555.system.heartbeat.ServerHeartbeatManager;
import cs555.system.metadata.ServerMetadata;
import cs555.system.metadata.ServerMetadata.ChunkInformation;
import cs555.system.transport.TCPConnection;
import cs555.system.transport.TCPServerThread;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.FileUtilities;
import cs555.system.util.FileUtilities.ChunkIntegrityInformation;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.FailureChunkRead;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.ReadChunkRequest;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.RedirectChunkRequest;
import cs555.system.wireformats.RegisterResponse;
import cs555.system.wireformats.WriteChunkRequest;

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

  private final ServerMetadata metadata;

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
    this.metadata = new ServerMetadata( host + ":" + Integer.toString( port ) );
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
    LOG.info( "Chunk server starting up at: " + new Date() );
    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      ChunkServer node =
          new ChunkServer( InetAddress.getLocalHost().getHostName(),
              serverSocket.getLocalPort() );

      ( new Thread( new TCPServerThread( node, serverSocket ),
          "Server Thread" ) ).start();

      node.controllerConnection = ConnectionUtilities.registerNode( node,
          Constants.CHUNK_ID, Constants.CONTROLLER_HOST,
          Integer.valueOf( Constants.CONTROLLER_PORT ) );

      ServerHeartbeatManager serverHeartbeatManager =
          new ServerHeartbeatManager( node.controllerConnection,
              node.metadata );
      Timer timer = new Timer();
      final int interval = 30 * 1000; // 30 seconds in milliseconds
      timer.schedule( serverHeartbeatManager, 1000, interval );

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
          ConnectionUtilities.unregisterNode( this, Constants.CHUNK_ID,
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

      case Protocol.WRITE_CHUNK_REQUEST :
        writeChunkHandler( event );
        break;

      case Protocol.READ_CHUNK_REQUEST :
        readChunkHandler( event, connection );
        break;

      case Protocol.REDIRECT_CHUNK_REQUEST :
        redirectChunkHandler( event );
    }
  }

  /**
   * Upon a server failing, redirected chunks are sent to source nodes
   * with legitimate copies of date and forwarded as a replication to
   * another server.
   * 
   * The integrity information is not removed when redirecting chunks
   * from failed server. Instead, the entire chunk is sent.
   * 
   * @param event
   */
  private void redirectChunkHandler(Event event) {
    RedirectChunkRequest redirectRequest = ( RedirectChunkRequest ) event;
    String filename = redirectRequest.getFilename();
    int sequence = redirectRequest.getSequence();

    Path path = FileUtilities.getPathLocation( this, filename, sequence );
    byte[] message = FileUtilities.readChunkSequence( path );
    if ( message != null )
    {
      try
      {
        String[] destination =
            redirectRequest.getDestinationDetails().split( ":" );
        TCPConnection connection = ConnectionUtilities.establishConnection(
            this, destination[ 0 ], Integer.parseInt( destination[ 1 ] ) );

        ChunkInformation info =
            metadata.getChunkInformation( filename, sequence );

        WriteChunkRequest writeRequest = new WriteChunkRequest( filename,
            sequence, message, info.getLastModifiedTime(), info.getVersion(),
            new String[] { "" } );

        writeRequest
            .setReplicationPosition( redirectRequest.getReplicationPosition() );

        connection.getTCPSender().sendData( writeRequest.getBytes() );
        connection.close();
      } catch ( NumberFormatException | IOException | InterruptedException e )
      {
        LOG.error( "Unable to forward the request for " + filename + ", "
            + e.getMessage() );
        e.printStackTrace();
      }
    } else
    {
      LOG.error(
          "Chunk was not found on source node, and can not be forwarded to"
              + " the destination." );
    }
  }

  /**
   * Process an incoming chunk by saving it to disk and forwarding the
   * message to the other chunk servers.
   * 
   * Prior to writing the chunk to disk the integrity of the chunk is
   * computed in slices with SHA-1, and prepend to the beginning. This
   * only occurs on the first chunk server.
   * 
   * @param event
   */
  private void writeChunkHandler(Event event) {
    WriteChunkRequest request = ( WriteChunkRequest ) event;
    try
    {
      byte[] message = request.getMessage();
      if ( message.length == Constants.CHUNK_SIZE )
      {
        message = FileUtilities.addSHA1Integrity( message );
        request.setMessage( message );
      }

      Path path = FileUtilities.getPathLocation( this, request.getFilename(),
          request.getSequence() );

      long lastModifiedDate = request.getLastModifiedDate();

      ChunkInformation info = metadata
          .getChunkInformation( request.getFilename(), request.getSequence() );

      if ( info != null )
      {
        if ( !FileUtilities.messageIntegrityMatchesDisk( path, message ) )
        {
          info.incrementVersion();
          info.setLastModifiedDate( lastModifiedDate );

          Files.createDirectories( path.getParent() );
          Files.write( path, message );
          LOG.info( "Finished writing an updated version of "
              + request.getFilename() + " to disk." );
        } else
        {
          LOG.debug(
              "The chunk was not updated because the content is the same." );
        }
      } else
      {
        Files.createDirectories( path.getParent() );
        Files.write( path, message );
        LOG.info( "Finished writing " + request.getFilename() + " to disk." );

        metadata.update( request.getFilename(), request.getSequence(),
            request.getReplicationPosition(), lastModifiedDate,
            request.getVersion() );
      }
    } catch ( IOException e )
    {
      LOG.error( "Unable to save chunk " + request.getFilename() + " to disk. "
          + e.getMessage() );
      e.printStackTrace();
    }

    forwardIncomingChunk( request );
  }

  /**
   * Increment the position within the request and forward to the next
   * server if applicable.
   * 
   * @param request to forward
   */
  private void forwardIncomingChunk(WriteChunkRequest request) {
    request.incrementReplicationPosition();
    if ( request.getReplicationPosition() < request.getRoutingPath().length )
    {
      try
      {
        String[] nextChunkServer =
            request.getRoutingPath()[ request.getReplicationPosition() ]
                .split( ":" );
        TCPConnection connection =
            ConnectionUtilities.establishConnection( this, nextChunkServer[ 0 ],
                Integer.parseInt( nextChunkServer[ 1 ] ) );

        connection.getTCPSender().sendData( request.getBytes() );
        connection.close();
      } catch ( NumberFormatException | IOException | InterruptedException e )
      {
        LOG.error( "Unable to forward the request for " + request.getFilename()
            + ", " + e.getMessage() );
        e.printStackTrace();
      }
    }
  }

  /**
   * Validate the integrity of the chunk from disk and send to the
   * client.
   * 
   * The integrity of a chunk is recomputing using SHA-1 has for the
   * original chunk slices and comparing the array of hashes to the
   * persisted value on disk.
   * 
   * If it is detected that a chunk is corrupt, then the controller will
   * be messaged.
   * 
   * @param event
   * @param connection
   */
  private void readChunkHandler(Event event, TCPConnection connection) {
    ReadChunkRequest request = ( ReadChunkRequest ) event;
    byte[] message =
        FileUtilities.readChunkSequence( FileUtilities.getPathLocation( this,
            request.getFilename(), request.getSequence() ) );

    ChunkIntegrityInformation content =
        FileUtilities.validateSHA1Integrity( message );
    ReadChunkResponse response;
    if ( content.isValidChunk() )
    {
      response = new ReadChunkResponse( request.getFilename(),
          content.getMessage(), Constants.SUCCESS );
    } else
    {
      response =
          new ReadChunkResponse( request.getFilename(), Constants.FAILURE );
      LOG.debug(
          "Sent FailureSliceRead message to controller to fix failure." );
      FailureChunkRead failureRequest =
          new FailureChunkRead( this.getHost() + ":" + this.getPort(),
              request.getFilename(), request.getSequence() );
      try
      {
        controllerConnection.getTCPSender()
            .sendData( failureRequest.getBytes() );
      } catch ( IOException e )
      {
        LOG.error(
            "Unable to send request message to controller. " + e.getMessage() );
        e.printStackTrace();
      }
    }
    LOG.debug( "The status of the chunk read is: "
        + ( response.getStatus() == Constants.SUCCESS ? "successful."
            : "failed." ) );
    try
    {
      connection.getTCPSender().sendData( response.getBytes() );
      LOG.debug( "Sent ReadChunkRequest() message to client." );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send response message to client. " + e.getMessage() );
      e.printStackTrace();
    }
  }
}
