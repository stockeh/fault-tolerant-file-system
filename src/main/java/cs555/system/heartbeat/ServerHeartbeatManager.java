package cs555.system.heartbeat;

import java.io.IOException;
import java.util.TimerTask;
import cs555.system.metadata.ServerMetadata;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Logger;

/**
 * 
 * @author stock
 *
 */
public class ServerHeartbeatManager extends TimerTask {

  private final static Logger LOG = new Logger();

  private final TCPConnection controllerConnection;

  private final ServerMetadata metadata;

  private int counter;

  /**
   * Default constructor -
   * 
   * @param controllerConnection
   * @param metadata
   */
  public ServerHeartbeatManager(TCPConnection controllerConnection,
      ServerMetadata metadata) {
    this.controllerConnection = controllerConnection;
    this.metadata = metadata;
    this.counter = 0;
  }

  /**
   * 
   * Scheduled in a parent class similar with:
   * 
   * Timer timer = new Timer(); timer.schedule(this, 0, N);
   * 
   */
  @Override
  public void run() {
    try
    {
      byte[] message;

      if ( ++counter % 2 == 0 )
      {
        counter = 0;
        message = metadata.getMajorHeartbeatBytes();
      } else
      {
        message = metadata.getMinorHeartbeatBytes();
      }
      controllerConnection.getTCPSender().sendData( message );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send heartbeat message to controller. " + e.getMessage() );
      e.printStackTrace();
    }
  }
}
