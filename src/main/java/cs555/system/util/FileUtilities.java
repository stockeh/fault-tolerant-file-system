package cs555.system.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import cs555.system.node.Node;

/**
 * Utility class for all file related activities.
 * 
 * @author stock
 *
 */
public class FileUtilities {

  private static Logger LOG = new Logger();

  private final static int NUMBER_OF_SLICES =
      ( int ) Constants.CHUNK_SIZE / Constants.CHUNK_SLICE_SIZE;

  // 160 bit ( 20 byte ) hash length of digest returned
  private final static int SHA1_DIGEST_SIZE = 20;

  public final static int INTEGRITY_SIZE = SHA1_DIGEST_SIZE * NUMBER_OF_SLICES;

  public final static String ALGORITHM = "SHA1";


  /**
   * Adds integrity to the end of each slice. Increasing the size of the
   * message.
   * <p>
   * <tt>( slice_0 | integrity_0 | slice_1 | integrity_1 | ... )</tt>
   * </p>
   * 
   * @param message to add integrity information to
   * @return a message with the integrity information added
   */
  public static byte[] addSHA1Integrity(byte[] message) {
    MessageDigest digest = null;
    try
    {
      digest = MessageDigest.getInstance( ALGORITHM );
    } catch ( NoSuchAlgorithmException e )
    {
      LOG.error( "Unable to compute SHA-1 integrity for the written message. "
          + e.getMessage() );
      return null;
    }
    ByteBuffer buffer =
        ByteBuffer.allocate( INTEGRITY_SIZE + Constants.CHUNK_SIZE );

    for ( int i = 0; i < NUMBER_OF_SLICES; ++i )
    {
      byte[] slice =
          Arrays.copyOfRange( message, i * Constants.CHUNK_SLICE_SIZE,
              ( i + 1 ) * Constants.CHUNK_SLICE_SIZE );
      digest.update( slice );
      buffer.put( slice ).put( digest.digest() );
    }
    return buffer.array();
  }

  /**
   * Retrieves the original message if the integrity has not been
   * tampered with. Otherwise, returns a <tt>null</tt> message with the
   * malformed slice index.
   * 
   * <p>
   * <tt>( slice_0 | slice_1  | ... )</tt>
   * </p>
   * 
   * @param message to remove integrity from.
   * @return If the integrity is unchanged, then returns:
   *         <p>
   *         <tt>MessageInformation( original_message, -1 )</tt>
   *         </p>
   *         otherwise, a certain slice is invalid, returning:
   *         <p>
   *         <tt>MessageInformation( null, invalid_slice_index )</tt>
   *         </p>
   */
  public static MessageInformation removeSHA1Integrity(byte[] message) {
    MessageDigest digest = null;
    try
    {
      digest = MessageDigest.getInstance( ALGORITHM );
    } catch ( NoSuchAlgorithmException e )
    {
      LOG.error( "Unable to compute SHA-1 integrity for the written message. "
          + e.getMessage() );
      return null;
    }
    ByteBuffer buffer = ByteBuffer.allocate( Constants.CHUNK_SIZE );
    for ( int index = 0; index < NUMBER_OF_SLICES; ++index )
    {
      int offset = index * SHA1_DIGEST_SIZE;

      byte[] slice = Arrays.copyOfRange( message,
          index * Constants.CHUNK_SLICE_SIZE + offset,
          ( index + 1 ) * Constants.CHUNK_SLICE_SIZE + offset );

      byte[] integrity = Arrays.copyOfRange( message,
          ( index + 1 ) * Constants.CHUNK_SLICE_SIZE + offset,
          ( index + 1 ) * Constants.CHUNK_SLICE_SIZE + SHA1_DIGEST_SIZE );

      digest.update( slice );

      if ( Arrays.equals( integrity, digest.digest() ) )
      {
        buffer.put( slice );
      } else
      {
        return new MessageInformation( null, index );
      }
    }
    return new MessageInformation( buffer.array(), -1 );
  }

  /**
   * Read a chunk of a file denoted by the <tt>filename</tt> and
   * <tt>sequence</tt> number into a <tt>byte[]</tt>.
   * 
   * @param node added to write file name with connection details
   * @param filename
   * @param sequence
   * @return a <tt>byte[]</tt> of the original bytes written to disk.
   *         This includes the message and the integrity information.
   */
  public static byte[] readChunkSequence(Node node, String filename,
      int sequence) {
    byte[] message;
    try
    {
      message = Files.readAllBytes(
          FileUtilities.getPathLocation( node, filename, sequence ) );
    } catch ( IOException e )
    {
      LOG.error( "Unable to read chunk file: \'" + filename + "\', chunk: \'"
          + sequence + "\' from disk." );
      e.printStackTrace();
      return null;
    }
    return message;
  }

  /**
   * Get the path common location for writing and reading chunks on
   * disk.
   * 
   * <p>
   * <tt>/tmp/host_port/filename_chunksequence</tt> </br>
   * </br>
   * TODO: use StringBuilder instead for performance?
   * </p>
   * 
   * @param node
   * @param chunkServer
   * @param filename
   * @param sequence
   * @return the resulting <tt>Path</tt>
   */
  public static Path getPathLocation(Node node, String filename, int sequence) {
    StringBuilder sb = new StringBuilder().append( filename ).append( "_chunk" )
        .append( sequence ).append( "_" ).append( node.getHost() ).append( "_" )
        .append( node.getPort() );
    return Paths.get( File.separator, "tmp", sb.toString() );
  }

  public static class MessageInformation {

    private byte[] message;

    private int invalidSliceIndex;

    private MessageInformation(byte[] message, int invalidSliceIndex) {
      this.message = message;
      this.invalidSliceIndex = invalidSliceIndex;
    }

    public byte[] getMessage() {
      return message;
    }

    public int getInvalidSliceIndex() {
      return invalidSliceIndex;
    }

  }

}
