package cs555.system.wireformats;

/**
 * Interface defining the wireformats between messaging nodes and the
 * registry.
 *
 * @author stock
 *
 */
public interface Protocol {

  final int REGISTER_REQUEST = 0;

  final int REGISTER_RESPONSE = 1;

  final int DEREGISTER_REQUEST = 2;

  final int MINOR_HEARTBEAT = 3;

  final byte SUCCESS = ( byte ) 200;

  final byte FAILURE = ( byte ) 500;

}
