package cs555.system.heartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import cs555.system.metadata.ControllerMetadata;
import cs555.system.metadata.ControllerMetadata.FileInformation;
import cs555.system.metadata.ControllerMetadata.ServerInformation;
import cs555.system.metadata.ControllerMetadata.ServerInformation.SequenceReplicationPair;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.FailureClientNotification;
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

  private List<FailedConnection> failedConnections;

  // TODO: does this need to be 1?
  private final static int NUMBER_OF_TASK_LOOPS = 1;

  /**
   * Default constructor -
   * 
   * @param metadata
   */
  public ControllerHeartbeatManager(ControllerMetadata metadata) {
    this.failedConnections = new ArrayList<>();
    this.metadata = metadata;
  }

  @Override
  public void run() {

    Map<String, ServerInformation> connections = metadata.getConnections();

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
        LOG.error( "Unable to send health request to chunk server. "
            + e.getMessage() );
        metadata.removeConnection( entry.getKey() );
        ServerInformation connection = entry.getValue();

        if ( !connection.getFilesOnServer().isEmpty() )
        {
          clearFileLocations( connection );
          failedConnections.add( new FailedConnection( connection ) );
        }
      }
    }
    // Only able to redirect information if there is more than one
    // replica, or enough connections to replicate. The client won't be
    // able to read if there is no more servers to replicate files...
    if ( !failedConnections.isEmpty() && Constants.NUMBER_OF_REPLICATIONS > 1
        && metadata.getConnections()
            .size() >= Constants.NUMBER_OF_REPLICATIONS )
    {
      // TODO: is this needed? The client will fail reading a chunk, and
      // move to the next...
      notifyClientsOfFailure();

      for ( FailedConnection failedConnection : new ArrayList<>(
          failedConnections ) )
      {
        if ( failedConnection.getIteration() >= NUMBER_OF_TASK_LOOPS )
        {
          processFailedConnection( failedConnection.getConnection() );
          failedConnections.remove( failedConnection );
        } else
        {
          failedConnection.incrementIteration();
        }
      }
    }
  }

  /**
   * Set <b>all</b> the chunk location for the failed connection to
   * null.
   * 
   * @param severInformation that failed
   */
  private void clearFileLocations(ServerInformation severInformation) {
    Map<String, List<SequenceReplicationPair>> files =
        severInformation.getFilesOnServer();
    for ( Entry<String, List<SequenceReplicationPair>> file : files.entrySet() )
    {
      // TODO: Send message to client to clear the given filename -
      // file.getKey()
      FileInformation info = metadata.getFiles().get( file.getKey() );
      String[][] chunks = info.getChunks();

      for ( SequenceReplicationPair pair : file.getValue() )
      {
        String identifier =
            chunks[ pair.getSequence() ][ pair.getReplication() ];
        if ( identifier != null )
        {
          chunks[ pair.getSequence() ][ pair.getReplication() ] = null;
        }
      }
    }
  }

  /**
   * Iterate over all files, and the chunks the failed connection had
   * for each file and find a free node that a replica can send the
   * information to.
   * 
   * @param severInformation that failed
   */
  private void processFailedConnection(ServerInformation serverInformation) {
    Map<String, List<SequenceReplicationPair>> files =
        serverInformation.getFilesOnServer();
    for ( Entry<String, List<SequenceReplicationPair>> file : files.entrySet() )
    {
      String filename = file.getKey();
      FileInformation info = metadata.getFiles().get( filename );
      String[][] chunks = info.getChunks();
      for ( SequenceReplicationPair pair : file.getValue() )
      {
        String source = null;
        for ( int replication =
            0; replication < Constants.NUMBER_OF_REPLICATIONS; ++replication )
        {
          String identifier = chunks[ pair.getSequence() ][ replication ];
          // Get a non-null source identifier. At this point the previous chunk
          // location was set to null.
          if ( identifier != null )
          {
            source = identifier;
            break;
          }
        }
        String destination =
            getDestination( chunks[ pair.getSequence() ], filename, pair );

        if ( source != null && destination != null )
        {
          TCPConnection connection =
              metadata.getConnections().get( source ).getConnection();

          RedirectChunkRequest request = new RedirectChunkRequest( filename,
              pair.getSequence(), pair.getReplication(), destination );
          try
          {
            connection.getTCPSender().sendData( request.getBytes() );
          } catch ( IOException e )
          {
            LOG.error( "Unable to send redirect request to chunk server. "
                + e.getMessage() );
            e.printStackTrace();
          }
        }
      }
    }
  }


  /**
   * Retrieve a single destination address that would best hold the
   * replicated file. This finds a server that does not already have the
   * replicated chunk.
   *
   * @param chunk array containing the replicated locations for the
   *        chunk
   * @param filename
   * @param pair sequence, replication location of the failed item.
   * @return a single destination host:port location
   */
  private String getDestination(String[] chunk, String filename,
      SequenceReplicationPair pair) {

    List<ServerInformation> availableConnections =
        new ArrayList<>( metadata.getConnections().values() );

    // see comparator for sort details
    Collections.sort( availableConnections, ControllerMetadata.COMPARATOR );

    Set<String> chunkSet = new HashSet<>( Arrays.asList( chunk ) );

    for ( ServerInformation info : availableConnections )
    {
      String connectionDetails = info.getConnectionDetails();
      if ( !chunkSet.contains( connectionDetails ) )
      {
        info.addFileOnServer( filename, pair.getSequence(),
            pair.getReplication() );
        info.incrementNumberOfChunks();
        return connectionDetails;
      }
    }
    return null;
  }

  /**
   * Send a message to all the connected clients to flush their metadata
   * and fetch an update of the available readable files.
   * 
   */
  private void notifyClientsOfFailure() {
    byte[] request = null;
    try
    {
      request = new FailureClientNotification().getBytes();
    } catch ( IOException e )
    {
      LOG.error( "Unable to create message to notify clients of failure. "
          + e.getMessage() );
      e.printStackTrace();
    }
    for ( TCPConnection connection : metadata.getClientConnections() )
    {
      try
      {
        connection.getTCPSender().sendData( request );
      } catch ( IOException e )
      {
        LOG.error( "Unable to contact the client. This makes the assumption it"
            + "disconnected ungracefully, so the connection will be removed. "
            + e.getMessage() );
        metadata.removeClientConnection( connection );
      }
    }
  }

  /**
   * Internal class to hold information about a failed connection and
   * the iteration count for the timer task related to this connection.
   * 
   * @author stock
   *
   */
  private static class FailedConnection {

    private final ServerInformation connection;

    private int iteration;

    private FailedConnection(ServerInformation connection) {
      this.connection = connection;
      this.iteration = 0;
    }

    private ServerInformation getConnection() {
      return connection;
    }

    private int getIteration() {
      return iteration;
    }

    private void incrementIteration() {
      ++iteration;
    }
  }
}
