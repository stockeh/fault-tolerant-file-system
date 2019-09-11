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

  // digest returns 20 byte hashes
  public final static int INTEGRITY_SIZE = 20 * NUMBER_OF_SLICES;

  /**
   * Computes the SHA-1 hash of a byte array as a 160 bit ( 20 byte )
   * digest.
   * 
   * @param data as an array of bytes
   * @return the hash digest from the data
   * @throws NoSuchAlgorithmException
   */
  public static byte[] SHA1FromBytes(byte[] message)
      throws NoSuchAlgorithmException, IllegalArgumentException {
    MessageDigest digest = null;
    final String algorithm = "SHA1";
    try
    {
      digest = MessageDigest.getInstance( algorithm );
    } catch ( NoSuchAlgorithmException e )
    {
      throw e;
    }

    ByteBuffer buffer = ByteBuffer.allocate( INTEGRITY_SIZE );
    for ( int i = 0; i < NUMBER_OF_SLICES; ++i )
    {
      digest.update( message, i * Constants.CHUNK_SLICE_SIZE,
          Constants.CHUNK_SLICE_SIZE );
      buffer.put( digest.digest() );
    }
    return buffer.array();
  }

  /**
   * Validate the integrity of a chunk file written to disk.
   * 
   * @param hash digest with integrity information
   * @return true if is valid, false other
   */
  public static boolean validateSHA1Integrity(byte[] message) {
    byte[] originalSHA1 = Arrays.copyOfRange( message, 0, INTEGRITY_SIZE );
    byte[] writtenMessage =
        Arrays.copyOfRange( message, INTEGRITY_SIZE, message.length );

    byte[] newSHA1;
    try
    {
      newSHA1 = SHA1FromBytes( writtenMessage );
    } catch ( NoSuchAlgorithmException | IllegalArgumentException e )
    {
      LOG.error( "Unable to compute SHA-1 integrity for the written message. "
          + e.getMessage() );
      return false;
    }
    return Arrays.equals( originalSHA1, newSHA1 );
  }

  /**
   * Read a chunk of a file denoted by the <tt>filename</tt> and
   * <tt>sequence</tt> number into a <tt>byte[]</tt>.
   * 
   * @param node added to write file name with connection details
   * @param filename
   * @param sequence
   * @return the resulting <tt>byte[]</tt> array, or null if
   *         unsuccessful
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
    if ( FileUtilities.validateSHA1Integrity( message ) )
    {
      return Arrays.copyOfRange( message, FileUtilities.INTEGRITY_SIZE,
          message.length );
    } else
    {
      return null;
    }
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

}
