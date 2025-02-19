package cs555.system.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from the controller to the client containing a list of all
 * the readable files.
 * 
 * @author stock
 *
 */
public class ListFileResponse implements Event {

  private int type;

  private List<String> filenames;

  /**
   * Default constructor -
   * 
   * @param filenames
   */
  public ListFileResponse(List<String> fileNames) {
    this.type = Protocol.LIST_FILE_RESPONSE;
    this.filenames = fileNames;
  }

  /**
   * Constructor - Unmarshall the <code>byte[]</code> to the respective
   * class elements.
   * 
   * @param marshalledBytes is the byte array of the class.
   * @throws IOException
   */
  public ListFileResponse(byte[] marshalledBytes) throws IOException {
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream( marshalledBytes );
    DataInputStream din =
        new DataInputStream( new BufferedInputStream( inputStream ) );

    this.type = din.readInt();

    int arrayLength = din.readInt();
    this.filenames = new ArrayList<>( arrayLength );

    for ( int i = 0; i < arrayLength; ++i )
    {
      int len = din.readInt();
      byte[] bytes = new byte[ len ];
      din.readFully( bytes );
      this.filenames.add( ( new String( bytes ) ) );
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
   * @return a list of file names that are available to read on the
   *         server
   */
  public List<String> getFileNames() {
    return filenames;
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

    dout.writeInt( filenames.size() );

    for ( String item : filenames )
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
    return "\n" + Integer.toString( type );
  }

}
