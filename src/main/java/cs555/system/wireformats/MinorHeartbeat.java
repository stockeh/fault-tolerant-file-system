package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import cs555.system.metadata.ServerMetadata.ChunkInformation;

/**
 * Message from the chunk server to the controller containing
 * information about the newly added chunks.
 * 
 * @author stock
 *
 */
public class MinorHeartbeat implements Event {

  private int type;

  private String connectionDetails;

  private int totalChunks;

  private long freeSpace;

  private Map<String, List<ChunkInformation>> files;

  private boolean isEmpty;


  /**
   * Default constructor - create a new task initiation
   * 
   * @param connectionDetails
   * @param totalChunks
   * @param freeSpace
   * @param files
   */
  public MinorHeartbeat(String connectionDetails, int totalChunks,
      long freeSpace, Map<String, List<ChunkInformation>> files) {
    this.type = Protocol.MINOR_HEARTBEAT;
    this.connectionDetails = connectionDetails;
    this.totalChunks = totalChunks;
    this.freeSpace = freeSpace;
    this.files = files;
    this.isEmpty = files.size() > 0 ? false : true;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public MinorHeartbeat(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] connectionDetailBytes = new byte[ len ];
    din.readFully( connectionDetailBytes );
    this.connectionDetails = new String( connectionDetailBytes );

    this.totalChunks = din.readInt();

    this.freeSpace = din.readLong();

    this.isEmpty = din.readBoolean();

    if ( !this.isEmpty )
    {
      // 1. read files length
      int numberOfFiles = din.readInt();
      this.files = new HashMap<>();

      for ( int i = 0; i < numberOfFiles; ++i )
      {
        // 2. read key
        len = din.readInt();
        byte[] bytes = new byte[ len ];
        din.readFully( bytes );
        String key = new String( bytes );

        // 3. read list length
        int numberOfChunks = din.readInt();
        List<ChunkInformation> value = new ArrayList<>( numberOfChunks );

        // 4. read each list item
        for ( int chunkNumber = 0; chunkNumber < numberOfChunks; ++chunkNumber )
        {
          int sequence = din.readInt();
          int position = din.readInt();
          value.add( new ChunkInformation( sequence, position ) );
        }
        this.files.put( key, value );
      }
    }
    inputStream.close();
    din.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getType() {
    return type;
  }

  /**
   * 
   * @return the host:ip combination from the connection
   */
  public String getConnectionDetails() {
    return connectionDetails;
  }

  /**
   * 
   * @return the number of chunks on the chunk server
   */
  public int getTotalChunks() {
    return totalChunks;
  }

  /**
   * 
   * @return the amount of free space on a chunk server
   */
  public long getFreeSpace() {
    return freeSpace;
  }

  /**
   * 
   * @return the files that were newly added to the chunk server
   */
  public Map<String, List<ChunkInformation>> getFiles() {
    return files;
  }

  /**
   * 
   * @return true if there are no new files with metadata, false
   *         otherwise
   */
  public boolean isEmpty() {
    return isEmpty;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( outputStream ) );

    dout.writeInt( type );

    byte[] connectionDetailBytes = connectionDetails.getBytes();
    dout.writeInt( connectionDetailBytes.length );
    dout.write( connectionDetailBytes );

    dout.writeInt( totalChunks );

    dout.writeLong( freeSpace );

    dout.writeBoolean( isEmpty );

    if ( !isEmpty )
    {
      // 1. write files length
      dout.writeInt( files.size() );

      for ( Entry<String, List<ChunkInformation>> entry : files.entrySet() )
      {
        // 2. write key
        byte[] bytes = entry.getKey().getBytes();
        dout.writeInt( bytes.length );
        dout.write( bytes );

        // 3. write list length
        List<ChunkInformation> value = entry.getValue();
        dout.writeInt( value.size() );

        // 4. write each list item
        for ( ChunkInformation info : value )
        {
          dout.writeInt( info.getSequence() );
          dout.writeInt( info.getReplication() );
        }
      }
    }
    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public String toString() {

    String extra = this.isEmpty ? ""
        : ", num files: " + Integer.toString( this.files.size() );

    return Integer.toString( this.type ) + ", connection details: "
        + this.connectionDetails + ", total chunks: "
        + Integer.toString( this.totalChunks ) + " " + ", free space: "
        + Long.toString( this.freeSpace ) + extra;
  }
}
