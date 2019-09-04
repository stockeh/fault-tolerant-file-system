package cs555.system.heartbeat;

import java.util.TimerTask;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Logger;

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

  @Override
  public void run() {

  }
}
