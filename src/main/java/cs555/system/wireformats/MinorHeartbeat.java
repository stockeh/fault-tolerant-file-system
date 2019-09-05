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
 * 
 * 
 * @author stock
 *
 */
public class MinorHeartbeat implements Event {

  private int type;

  private int totalChunks;

  private long freeSpace;

  private Map<String, List<ChunkInformation>> files;

  private boolean isEmpty;

  /**
   * Default constructor - create a new task initiation
   * 
   * @param totalChunks
   * @param freeSpace
   */
  public MinorHeartbeat(int totalChunks, long freeSpace,
      Map<String, List<ChunkInformation>> files) {
    this.type = Protocol.MINOR_HEARTBEAT;
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

    this.isEmpty = din.readBoolean();

    if ( !this.isEmpty )
    {
      this.totalChunks = din.readInt();

      this.freeSpace = din.readLong();

      // 1. read files length
      int numberOfFiles = din.readInt();
      this.files = new HashMap<>();

      for ( int i = 0; i < numberOfFiles; ++i )
      {
        // 2. read key
        int len = din.readInt();
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
   * @return The number of chunks on the chunk server
   */
  public int getTotalChunks() {
    return totalChunks;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getType() {
    return type;
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

    dout.writeBoolean( isEmpty );

    if ( !isEmpty )
    {
      dout.writeInt( totalChunks );

      dout.writeLong( freeSpace );

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
          dout.writeInt( info.getPosition() );
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
    return Integer.toString( this.type ) + " "
        + Integer.toString( this.totalChunks ) + " "
        + Long.toString( this.freeSpace );
  }

}
