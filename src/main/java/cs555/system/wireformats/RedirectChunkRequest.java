package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message request to chunk server to take an existing chunk and
 * forward it to a defined destination.
 * 
 * This is typically used when either (1) a server crashes and its
 * chunks need to be re-replicated, or (2) when a read request detects
 * invalid integrity information.
 * 
 * @author stock
 *
 */
public class RedirectChunkRequest implements Event {

  private int type;

  private String filename;

  private int sequence;

  /**
   * Used to set the previous chunks replication location
   */
  private int replicationPosition;

  private String destinationDetails;

  /**
   * Default constructor -
   * 
   * @param filename
   * @param sequence chunk number that is being updated
   * @param replicationPosition position of the replication for the
   *        failed chunk at the destination as seen by the controller.
   * @param destinationDetails
   */
  public RedirectChunkRequest(String filename, int sequence,
      int replicationPosition, String destinationDetails) {
    this.type = Protocol.REDIRECT_CHUNK_REQUEST;
    this.filename = filename;
    this.sequence = sequence;
    this.replicationPosition = replicationPosition;
    this.destinationDetails = destinationDetails;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public RedirectChunkRequest(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] bytes = new byte[ len ];
    din.readFully( bytes );
    this.filename = new String( bytes );

    this.sequence = din.readInt();

    this.replicationPosition = din.readInt();

    len = din.readInt();
    bytes = new byte[ len ];
    din.readFully( bytes );
    this.destinationDetails = new String( bytes );

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

  public String getFilename() {
    return filename;
  }

  public int getSequence() {
    return sequence;
  }

  public String getDestinationDetails() {
    return destinationDetails;
  }

  public int getReplicationPosition() {
    return replicationPosition;
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

    byte[] bytes = filename.getBytes();
    dout.writeInt( bytes.length );
    dout.write( bytes );

    dout.writeInt( sequence );

    dout.writeInt( replicationPosition );

    bytes = destinationDetails.getBytes();
    dout.writeInt( bytes.length );
    dout.write( bytes );

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
