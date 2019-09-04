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

  final int MAJOR_HEARTBEAT = 4;

  final int WRITE_CHUNK = 5;

  final int WRITE_QUERY = 6;

  final int WRITE_QUERY_RESPONSE = 7;

  final int READ_CHUNK = 8;

}
