package cs555.system.heartbeat;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.TimerTask;
import cs555.system.transport.TCPConnection;
import cs555.system.util.FileUtilities;
import cs555.system.util.Logger;
import cs555.system.wireformats.MinorHeartbeat;

/**
 * 
 * @author stock
 *
 */
public class ControllerHeartbeatManager extends TimerTask {

  private final static Logger LOG = new Logger();

  private final TCPConnection controllerConnection;

  public ControllerHeartbeatManager(TCPConnection controllerConnection) {
    this.controllerConnection = controllerConnection;
  }

  /**
   * Allows the client to print the number of messages it has sent and
   * received during the last N seconds.
   * 
   * Scheduled in a parent class similar with:
   * 
   * Timer timer = new Timer(); timer.schedule(this, 0, N);
   * 
   */
  @Override
  public void run() {

    int totalChunks = 0;
    long freeSpace = FileUtilities.calculateSize( Paths.get( "/tmp" ) );

    MinorHeartbeat msg = new MinorHeartbeat( totalChunks, freeSpace );
    try
    {
      controllerConnection.getTCPSender().sendData( msg.getBytes() );
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to send heartbeat message to controller. " + e.getMessage() );
      e.printStackTrace();
    }
  }
}
