package cs555.system.wireformats;

/**
 * Interface defining the wireformats between controller, client, and
 * chunk servers.
 *
 * @author stock
 *
 */
public interface Protocol {

  // Wireformats

  final int REGISTER_REQUEST = 0;

  final int REGISTER_RESPONSE = 1;

  final int UNREGISTER_REQUEST = 2;

  final int MINOR_HEARTBEAT = 3;

  final int MAJOR_HEARTBEAT = 4;

  final int WRITE_CHUNK = 5;

  final int WRITE_QUERY = 6;

  final int WRITE_QUERY_RESPONSE = 7;

  final int READ_CHUNK = 8;
  
  // Constants

  final int CLIENT_ID = 8;

  final int CHUNK_ID = 9;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

  final int CHUNK_SIZE = 64000;
  
  final int CHUNK_SLICE_SIZE = 8000;

}
