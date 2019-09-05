package cs555.system.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.wireformats.MinorHeartbeat;

/**
 * 
 * @author stock
 *
 */
public class ServerMetadata {

  private final AtomicInteger numberOfChunks;

  private final Map<String, List<ChunkInformation>> files;

  public ServerMetadata() {
    this.numberOfChunks = new AtomicInteger( 0 );
    this.files = new HashMap<>();
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

  public synchronized void update(String file, int sequence, int position) {

  }

  /**
   * Covert all the newly added temporary chunk data to a heartbeat
   * message, then clear the temporary metadata.
   * 
   * @return a wireformat representation of the newly added metadata.
   * @throws IOException
   */
  public synchronized byte[] getMinorHeartbeatBytes() throws IOException {
    MinorHeartbeat message =
        new MinorHeartbeat( getNumberOfChunks(), getFreeDiskSpace(), files );

    byte[] bytes = message.getBytes();
    files.clear();

    return bytes;
  }

  /**
   * 
   * 
   * @author stock
   *
   */
  public static class ChunkInformation {

    private int sequence;

    private int position;

    /**
     * Default constructor -
     * 
     * @param sequence of chunk in the file - this is the same as the
     *        chunk number
     * @param position of the chunk returned to the client form the
     *        controller
     */
    public ChunkInformation(int sequence, int position) {
      this.sequence = sequence;
      this.position = position;
    }

    /**
     * 
     * @return the sequence of chunk in the file - this is the same as the
     *         chunk number
     */
    public int getSequence() {
      return sequence;
    }

    /**
     * 
     * @return the position of the chunk returned to the client form the
     *         controller
     */
    public int getPosition() {
      return position;
    }

  }

}
