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
    // replica
    if ( !failedConnections.isEmpty() && Constants.NUMBER_OF_REPLICATIONS > 1
        && metadata.getConnections()
            .size() >= Constants.NUMBER_OF_REPLICATIONS )
    {
      Set<String> allConnectionDetails = metadata.getConnections().keySet();
      for ( ServerInformation connection : failedConnections )
      {
        Map<String, List<RedirectInformation>> redirectInformation =
            getRedirectInformation( connection, allConnectionDetails );
        if ( redirectInformation.size() > 0 )
        {
          // RecoverChunkRequest request = new
          // RecoverChunkRequest(redirectInformation);
          // TODO: send redirect info
        }
      }
    }
  }

  private Map<String, List<RedirectInformation>> getRedirectInformation(
      ServerInformation connection, Set<String> allConnectionDetails) {

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
            String destination = getDestination( chunks[ sequence ],
                allConnectionDetails, filename );

            if ( destination == null )
            {
              LOG.error( "There is no destination to send chunk too." );
              return null;
            }
            redirectInformation.putIfAbsent( source,
                new ArrayList<RedirectInformation>() );
            redirectInformation.get( filename )
                .add( new RedirectInformation( sequence, destination ) );
            metadata.getConnections().get( destination )
                .incrementNumberOfChunks();
            break;
          }
        }
      }
    }
    return redirectInformation;
  }

  private String getDestination(String[] chunk,
      Set<String> allConnectionDetails, String filename) {

    List<ServerInformation> serversWithoutChunk = new ArrayList<>();

    for ( String chunkConnectionDetails : chunk )
    {
      if ( !allConnectionDetails.contains( chunkConnectionDetails ) )
      {
        serversWithoutChunk
            .add( metadata.getConnections().get( chunkConnectionDetails ) );
      }
    }
    if ( serversWithoutChunk.size() > 0 )
    {
      Collections.sort( serversWithoutChunk, ControllerMetadata.COMPARATOR );
      ServerInformation destinationServer = serversWithoutChunk.get( 0 );

      destinationServer.addFileOnServer( filename );
      destinationServer.incrementNumberOfChunks();

      return destinationServer.getConnectionDetails();
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

    private int sequence;

    private String destinationDetails;

    /**
     * Default constructor -
     * 
     * @param sequence
     * @param destinationDetails
     */
    private RedirectInformation(int sequence, String destinationDetails) {
      this.sequence = sequence;
      this.destinationDetails = destinationDetails;
    }

    public int getSequence() {
      return sequence;
    }

    public String getDestinationDetails() {
      return destinationDetails;
    }

  }

}
