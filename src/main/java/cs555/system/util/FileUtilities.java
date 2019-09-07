package cs555.system.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Utility class for all file related activities.
 * 
 * @author stock
 *
 */
public class FileUtilities {

  private static Logger LOG = new Logger();

  // TODO: math.ceil?
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
      throws NoSuchAlgorithmException {
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
    } catch ( NoSuchAlgorithmException e )
    {
      LOG.error( "Unable to compute SHA-1 integrity for the written message."
          + e.getMessage() );
      return false;
    }
    return Arrays.equals( originalSHA1, newSHA1 );
  }

}
