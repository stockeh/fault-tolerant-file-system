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
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.HealthRequest;

/**
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
      for ( ServerInformation connection : failedConnections )
      {
        Map<String, List<RedirectInformation>> redirectInformation =
            getRedirectInformation( connection );
        if ( redirectInformation.size() > 0 )
        {
          for ( Entry<String, List<RedirectInformation>> entry : redirectInformation
              .entrySet() )
          {
            for ( RedirectInformation info : entry.getValue() )
            {
              LOG.debug( "source: " + entry.getKey() + ", filename: "
                  + info.getFilename() + ", sequence: " + info.getSequence()
                  + ", destination: " + info.getDestinationDetails() );
            }
          }
          // RecoverChunkRequest request = new
          // RecoverChunkRequest(redirectInformation);
          // TODO: send redirect info
        }
      }
    }
  }

  private Map<String, List<RedirectInformation>> getRedirectInformation(
      ServerInformation connection) {

    Map<String, List<RedirectInformation>> redirectInformation =
        new HashMap<>();

    String failedConnectionDetails = connection.getConnectionDetails();
    Set<String> filesOnServer = connection.getFilesOnServer();
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
            redirectInformation.get( source ).add(
                new RedirectInformation( filename, sequence, destination ) );
            break;
          }
        }
      }
    }
    return redirectInformation;
  }

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

    private String destinationDetails;

    /**
     * Default constructor -
     * 
     * @param filename
     * @param sequence
     * @param destinationDetails
     */
    private RedirectInformation(String filename, int sequence,
        String destinationDetails) {
      this.filename = filename;
      this.sequence = sequence;
      this.destinationDetails = destinationDetails;
    }

    public String getFilename() {
      return filename;
    }

    public int getSequence() {
      return sequence;
    }

    public String getDestinationDetails() {
      return destinationDetails;
    }
  }
}
