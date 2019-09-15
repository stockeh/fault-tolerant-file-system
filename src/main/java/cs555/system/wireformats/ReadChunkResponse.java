package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import cs555.system.util.Constants;

/**
 * Message from the chunk server to the client responding with the
 * status and message of the file sequence from disk.
 * 
 * @author stock
 *
 */
public class ReadChunkResponse implements Event {

  private int type;

  private String filename;

  private byte[] message;

  private byte status;

  /**
   * Constructor - for successful message
   * 
   * @param name
   * @param message
   * @param status
   */
  public ReadChunkResponse(String filename, byte[] message, byte status) {
    this.type = Protocol.READ_CHUNK_RESPONSE;
    this.filename = filename;
    this.message = message;
    this.status = status;
  }

  /**
   * Constructor - when a message contains tampered data.
   * 
   * @param filename
   * 
   * @param status
   */
  public ReadChunkResponse(String filename, byte status) {
    this.type = Protocol.READ_CHUNK_RESPONSE;
    this.filename = filename;
    this.status = status;
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

    this.status = din.readByte();

    int len = din.readInt();
    byte[] nameBytes = new byte[ len ];
    din.readFully( nameBytes );
    this.filename = new String( nameBytes );

    if ( status == Constants.SUCCESS )
    {
      int messageLength = din.readInt();
      this.message = new byte[ messageLength ];
      din.readFully( this.message );
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
   * 
   * @return the status of the read chunk response
   */
  public byte getStatus() {
    return status;
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

    dout.writeByte( status );

    byte[] nameBytes = filename.getBytes();
    dout.writeInt( nameBytes.length );
    dout.write( nameBytes );

    if ( status == Constants.SUCCESS )
    {
      dout.writeInt( message.length );
      dout.write( message );
    }
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
    return "\n" + Integer.toString( type ) + ", status: " + status;
  }

}
