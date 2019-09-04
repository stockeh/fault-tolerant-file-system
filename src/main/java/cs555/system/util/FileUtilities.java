package cs555.system.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import cs555.system.wireformats.Protocol;

/**
 * Utility class for all file related activities.
 * 
 * @author stock
 *
 */
public class FileUtilities {

  private final static int NUMBER_OF_SLICES =
      ( int ) Protocol.CHUNK_SIZE / Protocol.CHUNK_SLICE_SIZE;

  /**
   * Returns the size, in bytes, of the specified <tt>path</tt>. If the
   * given path is a regular file, trivially its size is returned. Else
   * the path is a directory and its contents are recursively explored,
   * returning the total sum of all files within the directory.
   * <p>
   * If an I/O exception occurs, it is suppressed within this method and
   * <tt>0</tt> is returned as the size of the specified <tt>path</tt>.
   * 
   * @param path path whose size is to be returned
   * @return size of the specified path
   */
  public static long calculateSize(Path path) {
    try
    {
      if ( Files.isRegularFile( path ) )
      {
        return Files.size( path );
      }

      return Files.list( path ).mapToLong( FileUtilities::calculateSize ).sum();
    } catch ( IOException e )
    {
      return 0L;
    }
  }

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
      digest.update( message, i * Protocol.CHUNK_SLICE_SIZE,
          Protocol.CHUNK_SLICE_SIZE );
      buffer.put( digest.digest() );
    }
    return buffer.array();
  }

  public static boolean checkSHA1Integrity() {
    return false;
  }
}
