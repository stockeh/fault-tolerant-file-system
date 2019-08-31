package cs555.system.wireformats;

/**
 * Interface defining the wireformats between controller, client, 
 * and chunk servers.
 *
 * @author stock
 *
 */
public interface Protocol {

  final int REGISTER_REQUEST = 0;

  final int REGISTER_RESPONSE = 1;

  final int DEREGISTER_REQUEST = 2;

  final int MINOR_HEARTBEAT = 3;
  
  final int WRITE_CHUNKS = 4;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

}
