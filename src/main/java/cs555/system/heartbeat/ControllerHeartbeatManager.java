package cs555.system.heartbeat;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import cs555.system.metadata.ControllerMetadata;
import cs555.system.metadata.ControllerMetadata.FileInformation;
import cs555.system.metadata.ControllerMetadata.ServerInformation;
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
    Set<ServerInformation> failedConnections = new HashSet<>();
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

    if ( !failedConnections.isEmpty() )
    {
      for ( ServerInformation connection : failedConnections )
      {
        List<String> filesOnServer = connection.getFilesOnServer();
        for ( String filename : filesOnServer )
        {
          FileInformation info = metadata.getFiles().get( filename );
          LOG.debug( Integer.toString( info.getFilelength() ) );
        }
      }
      // TODO: remove connection and update files to reflect chunk location
    }
  }
}
