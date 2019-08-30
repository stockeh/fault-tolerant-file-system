package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 
 * 
 * @author stock
 *
 */
public class MinorHeartbeat implements Event {

  int type;

  int totalChunks;

  long freeSpace;

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

    this.totalChunks = din.readInt();

    this.freeSpace = din.readLong();

    inputStream.close();
    din.close();
  }

  /**
   * Default constructor - create a new task initiation
   * 
   * @param totalChunks
   */
  public MinorHeartbeat(int totalChunks, long freeSpace) {
    this.type = Protocol.MINOR_HEARTBEAT;
    this.totalChunks = totalChunks;
    this.freeSpace = freeSpace;
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

    dout.writeInt( totalChunks );

    dout.writeLong( freeSpace );

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
