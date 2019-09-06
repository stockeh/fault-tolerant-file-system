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
 * Request from the client to the controller requesting a list of
 * chunk servers to read chunk data from for a given file.
 * 
 * @author stock
 *
 */
public class ReadFileRequest implements Event {

  private int type;

  private String filename;

  /**
   * Default constructor -
   * 
   * @param fileName
   */
  public ReadFileRequest(String fileName) {
    this.type = Protocol.READ_FILE_REQUEST;
    this.filename = fileName;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public ReadFileRequest(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] filenameBytes = new byte[ len ];
    din.readFully( filenameBytes );
    this.filename = new String( filenameBytes );

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
   * @return the filename associated with a read request
   */
  public String getFilename() {
    return filename;
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

    byte[] filenameBytes = filename.getBytes();
    dout.writeInt( filenameBytes.length );
    dout.write( filenameBytes );

    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public String toString() {
    return "\n" + Integer.toString( type );
  }

}
