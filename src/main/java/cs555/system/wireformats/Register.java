package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Register message type to initialize itself with another node.
 * 
 * This is a reusable class for registering, and unregistering chunk
 * servers with the controller. As well as connecting messaging nodes
 * to other chunk servers to construct the overlay.
 * 
 * @author stock
 *
 */
public class Register implements Event {

  private int type;

  private int identifier;

  private String ipAddress;

  private int port;

  /**
   * Default constructor - create a new register or unregister message.
   * 
   * @param type Specified for use of register or unregister message.
   * @param clinetId to distinguish between a chunk server and a client.
   * @param ipAddress
   * @param port
   */
  public Register(int type, int identifier, String ipAddress, int port) {
    this.type = type;
    this.identifier = identifier;
    this.ipAddress = ipAddress;
    this.port = port;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public Register(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    this.identifier = din.readInt();

    int len = din.readInt();
    byte[] ipBytes = new byte[ len ];
    din.readFully( ipBytes );

    this.ipAddress = new String( ipBytes );

    this.port = din.readInt();

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
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes() throws IOException {
    byte[] marshalledBytes = null;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dout =
        new DataOutputStream( new BufferedOutputStream( outputStream ) );

    dout.writeInt( type );

    dout.writeInt( identifier );

    byte[] ipBytes = ipAddress.getBytes();
    dout.writeInt( ipBytes.length );
    dout.write( ipBytes );

    dout.writeInt( port );

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
    return Integer.toString( this.type ) + " " + this.getConnection();
  }

  /**
   * Converts the IP Address and Port to a readable format.
   * 
   * @return Returns a string in the format <code>host:port</code>
   */
  public String getConnection() {
    return this.ipAddress + ":" + Integer.toString( this.port );
  }

  /**
   * Retrieve the identifier for the controller message.
   * 
   * @return integer representing the id
   */
  public int getIdentifier() {
    return this.identifier;
  }
}
