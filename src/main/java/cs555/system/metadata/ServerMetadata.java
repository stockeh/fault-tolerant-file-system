package cs555.system.metadata;

import java.io.File;
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
   * @return the current number of chunks maintained by the chunk server
   */
  public int getNumberOfChunks() {
    return numberOfChunks.get();
  }

  /**
   * 
   * @return the free disk space in bytes of the root <tt>/</tt>
   *         directory
   */
  public long getFreeDiskSpace() {
    return ( new File( File.separator ) ).getFreeSpace();
  }

  public synchronized void clear() {

  }

}
