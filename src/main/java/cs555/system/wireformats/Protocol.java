package cs555.system.wireformats;

/**
 * Interface defining the wireformats between controller, client, and
 * chunk servers.
 *
 * @author stock
 *
 */
public interface Protocol {

  final int REGISTER_REQUEST = 0;

  final int REGISTER_RESPONSE = 1;

  final int UNREGISTER_REQUEST = 2;

  final int MINOR_HEARTBEAT = 3;

  final int WRITE_CHUNKS = 4;

  final int CLIENT_ID = 5;

  final int CHUNK_ID = 6;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

  final int CHUNK_SIZE = 64000;

}
