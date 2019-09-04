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
 * Forward data to the chunk servers.
 * 
 * Upon writing a new chuck to the chuck server, the client will send
 * a message to the chunk server with the message content and list of
 * nodes to forward the message. The chunk servers will reuse this
 * message to forward messages to other chunk servers.
 * 
 * @author stock
 *
 */
public class WriteChunk implements Event {

  private int type;

  private String path;

  private byte[] message;

  private String[] routes;

  private int position;

  private boolean isOriginalFile;

  public WriteChunk(String name, byte[] message, String[] routes) {
    this.type = Protocol.WRITE_CHUNK;
    this.path = name;
    this.message = message;
    this.routes = routes;
    this.position = 0;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public WriteChunk(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] nameBytes = new byte[ len ];
    din.readFully( nameBytes );
    this.path = new String( nameBytes );

    int messageLength = din.readInt();

    this.message = new byte[ messageLength ];

    din.readFully( this.message );

    int arrayLength = din.readInt();

    this.routes = new String[ arrayLength ];

    for ( int i = 0; i < arrayLength; ++i )
    {
      len = din.readInt();
      byte[] bytes = new byte[ len ];
      din.readFully( bytes );
      this.routes[ i ] = ( new String( bytes ) );
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
   * @return the chunk path location from the client
   */
  public String getPath() {
    return path;
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
   * @return the routing path decided from the controller
   */
  public String[] getRoutingPath() {
    return routes;
  }

  /**
   * 
   * @return the current position in the array of routes
   */
  public int getPosition() {
    return position;
  }

  /**
   * Increment the position for the next connection
   */
  public void incrementPosition() {
    ++position;
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

    byte[] nameBytes = path.getBytes();
    dout.writeInt( nameBytes.length );
    dout.write( nameBytes );

    dout.writeInt( message.length );
    dout.write( message );

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
    return "\n" + Integer.toString( type ) + ", chunk name: " + path
        + ", routes: " + Arrays.toString( routes ) + ", msg len: "
        + Integer.toString( message.length );
  }

}
