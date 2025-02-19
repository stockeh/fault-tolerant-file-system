package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Response message from the controller to the client containing chunk
 * server details upon a write query being received.
 * 
 * @author stock
 *
 */
public class WriteFileResponse implements Event {

  private int type;

  private int sequence;

  private String[] routes;

  private boolean ableToWrite;

  /**
   * Default constructor -
   * 
   * @param routes
   * @param sequence
   */
  public WriteFileResponse(String[] routes, int sequence) {
    this.type = Protocol.WRITE_FILE_RESPONSE;
    this.sequence = sequence;
    this.routes = routes;
    this.ableToWrite = routes == null ? false : true;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public WriteFileResponse(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    this.sequence = din.readInt();

    this.ableToWrite = din.readBoolean();

    if ( this.ableToWrite )
    {
      int arrayLength = din.readInt();

      this.routes = new String[ arrayLength ];

      for ( int i = 0; i < arrayLength; ++i )
      {
        int len = din.readInt();
        byte[] bytes = new byte[ len ];
        din.readFully( bytes );
        this.routes[ i ] = ( new String( bytes ) );
      }
    }
    inputStream.close();
    din.close();
  }

  public String[] getRoutingPath() {
    return routes;
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
   * @return true when able to write, false otherwise.
   */
  public boolean isAbleToWrite() {
    return ableToWrite;
  }

  /**
   * 
   * @return the sequence associated with a file
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

    dout.writeInt( sequence );

    dout.writeBoolean( ableToWrite );

    if ( ableToWrite )
    {
      dout.writeInt( routes.length );

      for ( String item : routes )
      {
        byte[] bytes = item.getBytes();
        dout.writeInt( bytes.length );
        dout.write( bytes );
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
    return "\n" + type + ", routes: "
        + ( ableToWrite ? Arrays.toString( routes ) : "no routes." );
  }

}
