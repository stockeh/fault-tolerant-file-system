package cs555.system.heartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import cs555.system.metadata.ControllerMetadata;
import cs555.system.metadata.ControllerMetadata.FileInformation;
import cs555.system.metadata.ControllerMetadata.ServerInformation;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.HealthRequest;
import cs555.system.wireformats.RedirectChunkRequest;

/**
 * 
 * A heartbeat is sent from the controller to the chunk servers to
 * detect server failures.
 * 
 * When a server failure is detected, the chunks on those servers will
 * be dispersed to other servers that do not already have those
 * replicas.
 * 
 * @author stock
 *
 */
public class ControllerHeartbeatManager extends TimerTask {

  private final static Logger LOG = new Logger();

  private ControllerMetadata metadata;

  /**
   * Default constructor -
   * 
   * @param metadata
   */
  public ControllerHeartbeatManager(ControllerMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void run() {

    Map<String, ServerInformation> connections = metadata.getConnections();
    List<ServerInformation> failedConnections = new ArrayList<>();

    byte[] request;
    try
    {
      request = ( new HealthRequest() ).getBytes();
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to create health request, so no health checks will be made. "
              + e.getMessage() );
      return;
    }
    for ( Entry<String, ServerInformation> entry : connections.entrySet() )
    {
      try
      {
        entry.getValue().getConnection().getTCPSender().sendData( request );
        LOG.debug( "Health request sent to chunk server." );
      } catch ( IOException e )
      {
        // TODO: is catching an exception good enough, or should I expect a
        // response?
        LOG.error( "Unable to send health request to chunk server." );
        failedConnections.add( entry.getValue() );
        metadata.removeConnection( entry.getKey() );
      }
    }
    // Only able to redirect information if there is more than one
    // replica, or enough connections to replicate
    if ( !failedConnections.isEmpty() && Constants.NUMBER_OF_REPLICATIONS > 1
        && metadata.getConnections()
            .size() >= Constants.NUMBER_OF_REPLICATIONS )
    {
      processFailedConnections( failedConnections );
    }
  }

  /**
   * Process the failed connections by locating chunks that hold
   * legitimate copies of the unaffected chunks, and have them send
   * these chunks to other destinations.
   * 
   * @param failedConnections
   */
  private void processFailedConnections(
      List<ServerInformation> failedConnections) {
    for ( ServerInformation serverInfo : failedConnections )
    {
      Map<String, List<RedirectInformation>> redirectInformation =
          getRedirectInformation( serverInfo );
      if ( redirectInformation.size() > 0 )
      {
        for ( Entry<String, List<RedirectInformation>> entry : redirectInformation
            .entrySet() )
        {
          TCPConnection serverConnection =
              metadata.getConnections().get( entry.getKey() ).getConnection();

          for ( RedirectInformation info : entry.getValue() )
          {
            RedirectChunkRequest redirectRequest = new RedirectChunkRequest(
                info.getFilename(), info.getSequence(),
                info.getReplicationPosition(), info.getDestinationDetails() );
            try
            {
              serverConnection.getTCPSender()
                  .sendData( redirectRequest.getBytes() );
            } catch ( IOException e )
            {
              LOG.error( "Unable to send redirect request to chunk server. "
                  + e.getMessage() );
              e.printStackTrace();
            }
          }
        }
        // TODO: Send request to clients clearing the ClientMetadata of any
        // known readable files, forcing the client to get a list of files.
      }
    }
  }

  /**
   * Obtain the redirection information for all the chunks from one
   * server.
   * 
   * @param serverInfo of the failed chunk server
   * @return a map with
   *         <tt>k: source, v: List(filename, sequence, destination)</tt>
   */
  private Map<String, List<RedirectInformation>> getRedirectInformation(
      ServerInformation serverInfo) {

    Map<String, List<RedirectInformation>> redirectInformation =
        new HashMap<>();

    String failedConnectionDetails = serverInfo.getConnectionDetails();
    Set<String> filesOnServer = serverInfo.getFilesOnServer();
    for ( String filename : filesOnServer )
    {
      FileInformation info = metadata.getFiles().get( filename );
      String[][] chunks = info.getChunks();
      for ( int sequence = 0; sequence < chunks.length; ++sequence )
      {
        for ( int replication =
            0; replication < Constants.NUMBER_OF_REPLICATIONS; ++replication )
        {
          if ( chunks[ sequence ][ replication ]
              .equals( failedConnectionDetails ) )
          {
            // Source is the next replication.
            // This assumes the next replication has not failed either
            String source = chunks[ sequence ][ ( replication + 1 )
                % Constants.NUMBER_OF_REPLICATIONS ];
            String destination = getDestination( chunks[ sequence ], filename );
            if ( destination == null )
            {
              LOG.error( "There is no destination to send chunk too." );
              return redirectInformation;
            }
            // Set chunk location for chunk to null and wait for heartbeat to
            // update
            chunks[ sequence ][ replication ] = null;
            redirectInformation.putIfAbsent( source,
                new ArrayList<RedirectInformation>() );
            redirectInformation.get( source ).add( new RedirectInformation(
                filename, sequence, replication, destination ) );
            break;
          }
        }
      }
    }
    return redirectInformation;
  }

  /**
   * Retrieve a single destination address that would best hold the
   * replicated file. This finds a server that does not already have the
   * replicated chunk.
   * 
   * @param chunk array containing the replicated locations for the
   *        chunk
   * @param filename
   * @return a single destination host:port location
   */
  private String getDestination(String[] chunk, String filename) {

    List<ServerInformation> list =
        new ArrayList<>( metadata.getConnections().values() );

    // see comparator for sort details
    Collections.sort( list, ControllerMetadata.COMPARATOR );

    Set<String> chunkSet = new HashSet<>( Arrays.asList( chunk ) );

    for ( ServerInformation info : list )
    {
      String connectionDetails = info.getConnectionDetails();
      if ( !chunkSet.contains( connectionDetails ) )
      {
        info.addFileOnServer( filename );
        info.incrementNumberOfChunks();
        return connectionDetails;
      }
    }
    return null;
  }

  /**
   * Class containing redirection information for a specific chunk.
   * 
   * @author stock
   *
   */
  private static class RedirectInformation {

    private String filename;

    private int sequence;

    private int replicationPosition;

    private String destinationDetails;

    /**
     * Default constructor -
     * 
     * @param filename
     * @param sequence
     * @param replicationPosition
     * @param destinationDetails
     */
    private RedirectInformation(String filename, int sequence,
        int replicationPosition, String destinationDetails) {
      this.filename = filename;
      this.sequence = sequence;
      this.replicationPosition = replicationPosition;
      this.destinationDetails = destinationDetails;
    }

    public String getFilename() {
      return filename;
    }

    public int getSequence() {
      return sequence;
    }

    public int getReplicationPosition() {
      return replicationPosition;
    }

    public String getDestinationDetails() {
      return destinationDetails;
    }
  }
}
