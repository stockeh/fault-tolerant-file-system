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
public class WriteChunkRequest implements Event {

  private int type;

  private String filename;

  private int sequence;

  private byte[] message;

  private long lastModifiedDate;

  private String[] routes;

  private int replicationPosition;


  /**
   * Default constructor -
   * 
   * @param filename
   * @param sequence chunk number associated with the write
   * @param message
   * @param lastModifiedDate time the <b>file</b> was last modified
   * @param routes where to send the message to
   */
  public WriteChunkRequest(String filename, int sequence, byte[] message,
      long lastModifiedDate, String[] routes) {
    this.type = Protocol.WRITE_CHUNK_REQUEST;
    this.filename = filename;
    this.sequence = sequence;
    this.message = message;
    this.lastModifiedDate = lastModifiedDate;
    this.routes = routes;
    this.replicationPosition = 0;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public WriteChunkRequest(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] nameBytes = new byte[ len ];
    din.readFully( nameBytes );
    this.filename = new String( nameBytes );

    this.sequence = din.readInt();

    int messageLength = din.readInt();
    this.message = new byte[ messageLength ];
    din.readFully( this.message );

    this.lastModifiedDate = din.readLong();

    int arrayLength = din.readInt();
    this.routes = new String[ arrayLength ];

    for ( int i = 0; i < arrayLength; ++i )
    {
      len = din.readInt();
      byte[] bytes = new byte[ len ];
      din.readFully( bytes );
      this.routes[ i ] = ( new String( bytes ) );
    }

    this.replicationPosition = din.readInt();

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
   * @return the file name from the client
   */
  public String getFilename() {
    return filename;
  }

  /**
   * 
   * @return sequence of chunk in the file - this is the same as the
   *         chunk number
   */
  public int getSequence() {
    return sequence;
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
   * @return the time in milliseconds the <b>file</b> was last modified
   */
  public long getLastModifiedDate() {
    return lastModifiedDate;
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
  public int getReplicationPosition() {
    return replicationPosition;
  }

  /**
   * Update the content of the message
   * 
   * @param message
   */
  public void setMessage(byte[] message) {
    this.message = message;
  }

  public void setReplicationPosition(int replicationPosition) {
    this.replicationPosition = replicationPosition;
  }

  /**
   * Increment the position for the next connection
   */
  public void incrementReplicationPosition() {
    ++replicationPosition;
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

    dout.writeInt( sequence );

    dout.writeInt( message.length );
    dout.write( message );

    dout.writeLong( lastModifiedDate );

    dout.writeInt( routes.length );

    for ( String item : routes )
    {
      byte[] bytes = item.getBytes();
      dout.writeInt( bytes.length );
      dout.write( bytes );
    }

    dout.writeInt( replicationPosition );

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
    return "\n" + type + ", chunk name: " + filename + ", sequence: " + sequence
        + ", routes: " + Arrays.toString( routes ) + ", msg len: "
        + message.length + ", last modified: " + lastModifiedDate;
  }

}
