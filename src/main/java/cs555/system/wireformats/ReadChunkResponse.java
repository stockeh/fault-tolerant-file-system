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
public class ReadChunkResponse implements Event {

  private int type;

  private String filename;

  private byte[] message;

  /**
   * Default constructor -
   * 
   * @param name
   * @param message
   */
  public ReadChunkResponse(String filename, byte[] message) {
    this.type = Protocol.READ_CHUNK_RESPONSE;
    this.filename = filename;
    this.message = message;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public ReadChunkResponse(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] nameBytes = new byte[ len ];
    din.readFully( nameBytes );
    this.filename = new String( nameBytes );

    int messageLength = din.readInt();
    this.message = new byte[ messageLength ];
    din.readFully( this.message );

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
   * @return the chunk name from the client
   */
  public String getFilename() {
    return filename;
  }

  /**
   * 
   * @return the chunk content from the client
   */
  public byte[] getMessage() {
    return message;
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

    dout.writeInt( message.length );
    dout.write( message );

    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "\n" + Integer.toString( type ) + ", msg len: "
        + Integer.toString( message.length );
  }

}
