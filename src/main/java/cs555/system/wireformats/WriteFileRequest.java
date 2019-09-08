package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Request from the client to the controller requesting a list of
 * chunk servers to write chunks of a file too.
 * 
 * @author stock
 *
 */
public class WriteFileRequest implements Event {

  private int type;

  private String filename;

  private int numberOfChunks;

  private int filelength;

  public WriteFileRequest(String name, int filelength, int numberOfChunks) {
    this.type = Protocol.WRITE_FILE_REQUEST;
    this.filename = name;
    this.filelength = filelength;
    this.numberOfChunks = numberOfChunks;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public WriteFileRequest(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] bytes = new byte[ len ];
    din.readFully( bytes );
    this.filename = new String( bytes );

    this.numberOfChunks = din.readInt();

    this.filelength = din.readInt();

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
   * @return the name of the file
   */
  public String getFilename() {
    return filename;
  }

  /**
   * 
   * @return the number of chunks associated with a file
   */
  public int getNumberOfChunks() {
    return numberOfChunks;
  }

  /**
   * 
   * @return the length of the file being written
   */
  public int getFilelength() {
    return filelength;
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

    byte[] nameBytes = filename.getBytes();
    dout.writeInt( nameBytes.length );
    dout.write( nameBytes );

    dout.writeInt( numberOfChunks );

    dout.writeInt( filelength );

    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public String toString() {
    return "\n" + Integer.toString( type ) + ", file name: " + filename
        + ", number of chunks: " + Integer.toString( numberOfChunks )
        + ", file length: " + filelength;
  }

}
