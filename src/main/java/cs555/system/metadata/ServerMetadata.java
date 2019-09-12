package cs555.system.metadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.wireformats.MinorHeartbeat;

/**
 * Maintains information about the chunk server and the newly added
 * chunks associated with a file ( on this servver ).
 * 
 * @author stock
 *
 */
public class ServerMetadata {

  private final AtomicInteger numberOfChunks;

  /**
   * map <k: filename, v: List<(sequence, replication)>>
   */
  private final Map<String, List<ChunkInformation>> newlyAddedFiles;

  private final String connectionDetails;

  /**
   * Default constructor -
   * 
   * @param connectionDetails
   */
  public ServerMetadata(String connectionDetails) {
    this.connectionDetails = connectionDetails;
    this.numberOfChunks = new AtomicInteger( 0 );
    this.newlyAddedFiles = new HashMap<>();
  }

  /**
   * 
   * @return the host:string identifier associated with the server
   */
  public String getConnectionDetails() {
    return connectionDetails;
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

  /**
   * Increment the <b>number of chunks</b> for a given chunk server.
   * 
   */
  public void incrementNumberOfChunks() {
    numberOfChunks.incrementAndGet();
  }

  /**
   * Update metadata associated with a file
   * 
   * @param filename
   * @param sequence
   * @param replication
   * @param fileTime 
   */
  public synchronized void update(String filename, int sequence,
      int replication, long timestamp) {
    newlyAddedFiles.putIfAbsent( filename, new ArrayList<ChunkInformation>() );
    newlyAddedFiles.get( filename )
        .add( new ChunkInformation( sequence, replication ) );
    incrementNumberOfChunks();
  }

  /**
   * Covert all the newly added temporary chunk data to a heartbeat
   * message, then clear the temporary metadata.
   * 
   * @return a wireformat representation of the newly added metadata.
   * @throws IOException
   */
  public synchronized byte[] getMinorHeartbeatBytes() throws IOException {
    MinorHeartbeat message = new MinorHeartbeat( getConnectionDetails(),
        getNumberOfChunks(), getFreeDiskSpace(), newlyAddedFiles );

    byte[] bytes = message.getBytes();
    newlyAddedFiles.clear();

    return bytes;
  }

  /**
   * 
   * 
   * @author stock
   *
   */
  public static class ChunkInformation {

    /**
     * Chunk number
     */
    private int sequence;

    /**
     * Replication position
     */
    private int replication;

    private int version;

    private long timestamp;

    /**
     * Default constructor -
     * 
     * @param sequence of chunk in the file - this is the same as the
     *        chunk number
     * @param replication of the chunk returned to the client form the
     *        controller
     */
    public ChunkInformation(int sequence, int replication) {
      this.sequence = sequence;
      this.replication = replication;
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
    public int getReplication() {
      return replication;
    }

    /**
     * Increment the version number for the file
     */
    public void incrementVersion() {
      ++version;
    }

  }

}
