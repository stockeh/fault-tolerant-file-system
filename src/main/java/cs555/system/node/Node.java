package cs555.system.node;

import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Event;

/**
 * Interface for the chunk server, client and controller, so
 * underlying communication is indistinguishable, i.e., Nodes send
 * messages to Nodes.
 * 
 * @author stock
 *
 */
public interface Node {

  /**
   * Gives the ability for events to be triggered by incoming messages
   * on a given node.
   * 
   * @param event
   * @param connection
   */
  public void onEvent(Event event, TCPConnection connection);

  /**
   * Host the node has been started on.
   * 
   * @return host
   */
  public String getHost();

  /**
   * Port the node has been starting on.
   * 
   * @return port
   */
  public int getPort();
}
