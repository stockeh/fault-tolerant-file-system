package cs555.system.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.wireformats.Heartbeat;
import cs555.system.wireformats.Protocol;

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

  /**
   * map <k: filename, v: List<(sequence, replication)>>
   */
  private final Map<String, List<ChunkInformation>> files;

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
    this.files = new HashMap<>();
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
   * Decrement the <b>number of chunks</b> for a given chunk server.
   * 
   */
  public void decrementNumberOfChunks() {
    numberOfChunks.getAndDecrement();
  }

  /**
   * Update metadata associated with a file <b>only</b> when an
   * <b>original</b> chunk is written.
   * 
   * @param filename
   * @param sequence of chunk in the file - this is the same as the
   *        chunk number
   * @param replication of the chunk returned to the client form the
   *        controller
   * @param lastModifiedDate the initial modified time of the chunk in
   *        milliseconds
   * @param version of the chunk as detected by the server
   */
  public synchronized void update(String filename, int sequence,
      int replication, long lastModifiedDate, int version) {

    newlyAddedFiles.putIfAbsent( filename, new ArrayList<ChunkInformation>() );
    newlyAddedFiles.get( filename ).add( new ChunkInformation( sequence,
        replication, lastModifiedDate, version ) );

    files.putIfAbsent( filename, new ArrayList<ChunkInformation>() );
    files.get( filename ).add( new ChunkInformation( sequence, replication,
        lastModifiedDate, version ) );

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
    Heartbeat message =
        new Heartbeat( Protocol.MINOR_HEARTBEAT, getConnectionDetails(),
            getNumberOfChunks(), getFreeDiskSpace(), newlyAddedFiles );

    byte[] bytes = message.getBytes();
    newlyAddedFiles.clear();

    return bytes;
  }

  /**
   * Covert all chunk data to a heartbeat message. Clear the newly added
   * files because major update takes care of this.
   * 
   * @return a wireformat representation of the newly added metadata.
   * @throws IOException
   */
  public synchronized byte[] getMajorHeartbeatBytes() throws IOException {
    Heartbeat message =
        new Heartbeat( Protocol.MAJOR_HEARTBEAT, getConnectionDetails(),
            getNumberOfChunks(), getFreeDiskSpace(), files );
    newlyAddedFiles.clear();

    return message.getBytes();
  }

  /**
   * Check if the server has knowledge about a given chunk for a some
   * file.
   * 
   * @param filename to search for
   * @param sequence number associated with the chunk
   * @return the chunk information if the chunk has been previously
   *         received, null otherwise
   */
  public synchronized ChunkInformation getChunkInformation(String filename,
      int sequence) {
    List<ChunkInformation> info = files.get( filename );
    if ( info == null )
    {
      return null;
    }
    return info.stream().filter( o -> o.getSequence() == sequence ).findFirst()
        .orElse( null );
  }
  
  /**
   * Remove the sequence metadata from the known files on the server.
   * 
   * This occurs when a read failure has invalid or corrupted data.
   * 
   * @param filename to search for
   * @param sequence number associated with the chunk to remove
   * @return true if removed, false otherwise
   */
  public synchronized boolean removeChunkInformation(String filename,
      int sequence) {
    List<ChunkInformation> info = files.get( filename );
    if ( info == null )
    {
      return false;
    }
    decrementNumberOfChunks();
    return info.removeIf(o -> o.getSequence() == sequence);
  }

  /**
   * Chunk information that is maintained for every chunk on the chunk
   * server.
   * 
   * @author stock
   *
   */
  public static class ChunkInformation {

    private int sequence;

    private int replication;

    private int version;

    private long lastModifiedDate;

    /**
     * Major constructor -
     * 
     * @param sequence of chunk in the file - this is the same as the
     *        chunk number
     * @param replication of the chunk returned to the client form the
     *        controller
     * @param lastModifiedDate the initial modified time of the chunk in
     *        milliseconds
     * @param version of the chunk as detected by the server
     */
    public ChunkInformation(int sequence, int replication,
        long lastModifiedDate, int verion) {
      this.sequence = sequence;
      this.replication = replication;
      this.lastModifiedDate = lastModifiedDate;
      this.version = verion;
    }

    /**
     * Minor constructor -
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
     * 
     * @return the last modified time of the chunk in milliseconds
     */
    public long getLastModifiedTime() {
      return lastModifiedDate;
    }

    /**
     * 
     * @return the version of the chunk
     */
    public int getVersion() {
      return version;
    }

    /**
     * Update the lastModifiedDate associated with the chunk based on the
     * last modified time.
     * 
     * @param lastModifiedDate
     */
    public void setLastModifiedDate(long lastModifiedDate) {
      this.lastModifiedDate = lastModifiedDate;
    }

    /**
     * Increment the version number for the file
     */
    public void incrementVersion() {
      ++version;
    }

  }

}
