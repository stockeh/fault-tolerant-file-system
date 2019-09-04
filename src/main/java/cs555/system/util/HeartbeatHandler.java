package cs555.system.util;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.MinorHeartbeat;

/**
 * Hold statistics for the chunk server that pertain to the number of
 * sent and received messages.
 * 
 * @author stock
 *
 */
public class HeartbeatHandler extends TimerTask {

  private final static Logger LOG = new Logger();

  private final AtomicInteger sent = new AtomicInteger( 0 );

  private final AtomicInteger received = new AtomicInteger( 0 );

  private final TCPConnection controllerConnection;

  public HeartbeatHandler(TCPConnection controllerConnection) {
    this.controllerConnection = controllerConnection;
  }

  /**
   * Increment the number of <b>sent</b> messages for a given client.
   * 
   */
  public void sent() {
    sent.incrementAndGet();
  }

  /**
   * Increment the number of <b>received</b> messages for a given
   * client.
   * 
   */
  public void received() {
    received.incrementAndGet();
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
    String timestamp =
        String.format( "%1$TF %1$TT", new Timestamp( new Date().getTime() ) );

    // System.out.println( "[" + timestamp + "]" + " Total Sent Count: "
    // + sent.get() + ", Total Received Count: " + received.get() + "\n"
    // );

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
    sent.set( 0 );
    received.set( 0 );
  }
}
