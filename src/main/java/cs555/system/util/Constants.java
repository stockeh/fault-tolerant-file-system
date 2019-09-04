package cs555.system.util;

/**
 * Interface defining the global constants between controller, client,
 * and chunk servers.
 *
 * @author stock
 *
 */
public interface Constants {

  final int CLIENT_ID = 0;

  final int CHUNK_ID = 1;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

  final int CHUNK_SIZE = 64000;

  final int CHUNK_SLICE_SIZE = 8000;

}
