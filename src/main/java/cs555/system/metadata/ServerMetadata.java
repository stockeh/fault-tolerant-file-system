package cs555.system.metadata;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author stock
 *
 */
public class ServerMetadata {

  private final AtomicInteger numberOfChunks;

  public ServerMetadata() {
    this.numberOfChunks = new AtomicInteger( 0 );
  }

  /**
   * Increment the <b>number of chunks</b> for a given chunk server.
   * 
   */
  public void incrementNumberOfChunks() {
    numberOfChunks.incrementAndGet();
  }

  /**
   * 
   * @return the current number of chunks mainted by the chunk server
   */
  public int getNumberOfChunks() {
    return numberOfChunks.get();
  }

  public void clear() {

  }

}
