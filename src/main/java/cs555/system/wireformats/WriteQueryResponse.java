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
public class WriteQueryResponse implements Event {

  private int type;

  private String[] routes;

  public WriteQueryResponse(String[] routes) {
    this.type = Protocol.WRITE_QUERY_RESPONSE;
    this.routes = routes;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public WriteQueryResponse(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int arrayLength = din.readInt();

    this.routes = new String[ arrayLength ];

    for ( int i = 0; i < arrayLength; ++i )
    {
      int len = din.readInt();
      byte[] bytes = new byte[ len ];
      din.readFully( bytes );
      this.routes[ i ] = ( new String( bytes ) );
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
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( outputStream ) );

    dout.writeInt( type );

    dout.writeInt( routes.length );

    for ( String item : routes )
    {
      byte[] bytes = item.getBytes();
      dout.writeInt( bytes.length );
      dout.write( bytes );
    }

    dout.flush();
    marshalledBytes = outputStream.toByteArray();

    outputStream.close();
    dout.close();
    return marshalledBytes;
  }

  @Override
  public String toString() {
    return "\n" + Integer.toString( type ) + " " + Arrays.toString( routes );
  }

}
