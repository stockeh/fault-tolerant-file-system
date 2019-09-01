package cs555.system.node;

import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Event;

/**
 * Interface for the MessagingNode and controller, so underlying
 * communication is indistinguishable, i.e., Nodes send messages to
 * Nodes.
 * 
 * @author stock
 *
 */
public interface Node {

  /**
   * Handle events delivered by messages
   * 
   * @param event
   * @param connection
   */
  public void onEvent(Event event, TCPConnection connection);

}
