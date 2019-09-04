package cs555.system.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for all file related activities.
 * 
 * @author stock
 *
 */
public class FileUtilities {

  private final static int NUMBER_OF_SLICES =
      ( int ) Constants.CHUNK_SIZE / Constants.CHUNK_SLICE_SIZE;

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

    // digest returns 20 byte hashes TODO: make global?
    ByteBuffer buffer = ByteBuffer.allocate( 20 * NUMBER_OF_SLICES );
    for ( int i = 0; i < NUMBER_OF_SLICES; ++i )
    {
      digest.update( message, i * Constants.CHUNK_SLICE_SIZE,
          Constants.CHUNK_SLICE_SIZE );
      buffer.put( digest.digest() );
    }
    return buffer.array();
  }

  public static boolean checkSHA1Integrity() {
    return false;
  }
}
