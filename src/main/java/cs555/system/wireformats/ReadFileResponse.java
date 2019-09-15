package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message from the controller to the client with a list of chunk
 * server locations for a request file.
 * 
 * @author stock
 *
 */
public class ReadFileResponse implements Event {

  private int type;

  private String filename;

  private int filelength;

  private String[][] chunks;

  /**
   * Default constructor -
   * 
   * @param filename
   * @param filelength
   * @param chunks sequence and chunk server information for a file
   */
  public ReadFileResponse(String filename, int filelength, String[][] chunks) {
    this.type = Protocol.READ_FILE_RESPONSE;
    this.filename = filename;
    this.filelength = filelength;
    this.chunks = chunks;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public ReadFileResponse(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int len = din.readInt();
    byte[] filenameBytes = new byte[ len ];
    din.readFully( filenameBytes );
    this.filename = new String( filenameBytes );

    this.filelength = din.readInt();

    int numChunks = din.readInt();
    int numReplications = din.readInt();

    this.chunks = new String[ numChunks ][ numReplications ];

    for ( int sequence = 0; sequence < numChunks; ++sequence )
    {
      for ( int replication = 0; replication < numReplications; ++replication )
      {
        len = din.readInt();
        if ( len == 0 )
        {
          chunks[ sequence ][ replication ] = null;
        } else
        {
          byte[] replicationIdentifier = new byte[ len ];
          din.readFully( replicationIdentifier );
          chunks[ sequence ][ replication ] =
              new String( replicationIdentifier );
        }
      }
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
   * @return the filename associated with a read request
   */
  public String getFilename() {
    return filename;
  }

  /**
   * 
   * @return the file length associated with a file
   */
  public int getFilelength() {
    return filelength;
  }

  /**
   * 
   * @return the chunks associated with a read response
   */
  public String[][] getChunks() {
    return chunks;
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

    dout.writeInt( filelength );

    dout.writeInt( chunks.length );
    dout.writeInt( chunks[ 0 ].length );

    for ( String[] chunkReplication : chunks )
    {
      for ( String replication : chunkReplication )
      {
        if ( replication == null )
        {
          dout.writeInt( 0 );
        } else
        {
          bytes = replication.getBytes();
          dout.writeInt( bytes.length );
          dout.write( bytes );
        }
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
    return "\n" + Integer.toString( type );
  }

}
