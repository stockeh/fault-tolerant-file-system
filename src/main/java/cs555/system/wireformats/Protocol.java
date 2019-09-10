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

  final int WRITE_FILE_REQUEST = 5;

  final int WRITE_FILE_RESPONSE = 6;

  final int WRITE_CHUNK_REQUEST = 7;

  final int LIST_FILE_REQUEST = 8;

  final int LIST_FILE_RESPONSE = 9;

  final int READ_FILE_REQUEST = 10;

  final int READ_FILE_RESPONSE = 11;

  final int READ_CHUNK_REQUEST = 12;

  final int READ_CHUNK_RESPONSE = 13;

  final int HEALTH_REQUEST = 14;

  final int REDIRECT_CHUNK_REQUEST = 15;

  final int FAILURE_CLIENT_NOTIFICATION = 16;

}
