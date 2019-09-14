package cs555.system.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import erasure.ReedSolomon;

/**
 * 
 * @author stock
 *
 */
public class ReedSolomonUtilities {

  /**
   * Covert a single chunk to an array of shards. This takes the
   * original chunk into {@link Constants#ERASURE_DATA_SHARDS}, and then
   * computes a total of {@link Constants#ERASURE_PARITY_SHARDS}.
   * 
   * @param bytes to convert to data and parity shards
   * @return a two-dimensional array the shards
   */
  public static byte[][] encode(byte[] bytes) {

    int bufferSize =
        Constants.ERASURE_SHARD_SIZE * Constants.ERASURE_DATA_SHARDS;

    byte[] buffer = ByteBuffer.allocate( bufferSize ).put( bytes ).array();

    byte[][] shards =
        new byte[ Constants.ERASURE_TOTAL_SHARDS ][ Constants.ERASURE_SHARD_SIZE ];

    for ( int i = 0; i < Constants.ERASURE_DATA_SHARDS; i++ )
    {
      System.arraycopy( buffer, i * Constants.ERASURE_SHARD_SIZE, shards[ i ],
          0, Constants.ERASURE_SHARD_SIZE );
    }

    ReedSolomon codec = new ReedSolomon( Constants.ERASURE_DATA_SHARDS,
        Constants.ERASURE_PARITY_SHARDS );
    codec.encodeParity( shards, 0, Constants.ERASURE_SHARD_SIZE );

    return shards;
  }

  /**
   * Convert all the shards into the original chunk content 
   * 
   * @param bytes returned by 
   * @return
   */
  public static byte[] decode(byte[][] bytes) {

    byte[][] shards =
        new byte[ Constants.ERASURE_TOTAL_SHARDS ][ Constants.ERASURE_SHARD_SIZE ];
    boolean[] shardPresent = new boolean[ Constants.ERASURE_TOTAL_SHARDS ];

    int shardCount = 0;

    for ( int i = 0; i < Constants.ERASURE_TOTAL_SHARDS; i++ )
    {
      // send request to chunk server if successful
      if ( true )
      {
        shards[ i ] = bytes[ i ];
        shardPresent[ i ] = true;
        ++shardCount;
      }
    }
    // Need at least DATA_SHARDS to be able to reconstruct the file
    if ( shardCount < Constants.ERASURE_DATA_SHARDS )
    {
      return null;
    }

    ReedSolomon codec = new ReedSolomon( Constants.ERASURE_DATA_SHARDS,
        Constants.ERASURE_PARITY_SHARDS );
    codec.decodeMissing( shards, shardPresent, 0,
        Constants.ERASURE_SHARD_SIZE );

    return shardsToArray( shards );
  }

  /**
   * 
   * @param shards
   * @return
   */
  public static byte[] shardsToArray(byte[][] shards) {
    byte[] bytes =
        new byte[ Constants.ERASURE_DATA_SHARDS * shards[ 0 ].length ];

    int k = 0;
    for ( int i = 0; i < Constants.ERASURE_DATA_SHARDS; i++ )
    {
      for ( int j = 0; j < shards[ i ].length; j++ )
      {
        bytes[ k++ ] = shards[ i ][ j ];
      }
    }
    return Arrays.copyOf( bytes, Constants.CHUNK_SIZE );
  }

  public static void main(String args[]) throws IOException {
    String filename =
        "/Users/stock/Development/cs/cs555/fault-tolerant-file-system/data/test.txt";

    File file = new File( filename );

    byte[] fileBytes = Files.readAllBytes( file.toPath() );

    byte[][] shards = encode( fileBytes );

    byte[] bytes = decode( shards );
  }
}
