package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message sent to the controller to retrieve a new slice from some
 * chunk to replace a corrupted one.
 * 
 * @author stock
 *
 */
public class FailureChunkRead implements Event {

  private int type;

  private String connectionDetails;

  private String filename;

  private int sequence;

  /**
   * Default constructor -
   * 
   * @param connectionDetails destination that contains the failed
   *        chunk.
   * @param filename
   * @param sequence chunk number that failed
   */
  public FailureChunkRead(String connectionDetails, String filename,
      int sequence) {
    this.type = Protocol.FAILURE_CHUNK_READ;
    this.connectionDetails = connectionDetails;
    this.filename = filename;
    this.sequence = sequence;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public FailureChunkRead(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] bytes = new byte[ len ];
    din.readFully( bytes );
    this.connectionDetails = new String( bytes );

    len = din.readInt();
    bytes = new byte[ len ];
    din.readFully( bytes );
    this.filename = new String( bytes );

    this.sequence = din.readInt();

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
   * @return the host:port connection details for the server
   */
  public String getConnectionDetails() {
    return connectionDetails;
  }

  /**
   * 
   * @return the chunk name from the client
   */
  public String getFilename() {
    return filename;
  }

  /**
   * 
   * @return the sequence ( chunk number ) for a given file
   */
  public int getSequence() {
    return sequence;
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

    byte[] bytes = connectionDetails.getBytes();
    dout.writeInt( bytes.length );
    dout.write( bytes );

    bytes = filename.getBytes();
    dout.writeInt( bytes.length );
    dout.write( bytes );

    dout.writeInt( sequence );

    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public String toString() {
    return "\n" + type + ", connection details: " + connectionDetails
        + ", filename: " + filename + ", sequence: " + sequence;
  }

}
