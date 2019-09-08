package cs555.system.wireformats;

import java.io.IOException;
import java.nio.ByteBuffer;
import cs555.system.util.Logger;

/**
 * Singleton class in charge of creating objects, i.e., messaging
 * types, from reading the first byte of a message.
 * 
 * @author stock
 *
 */
public class EventFactory {

  private static final Logger LOG = new Logger();

  private static final EventFactory instance = new EventFactory();

  /**
   * Default constructor - Exists only to defeat instantiation.
   */
  private EventFactory() {}

  /**
   * Single instance ensures that singleton instances are created only
   * when needed.
   * 
   * @return Returns the instance for the class
   */
  public static EventFactory getInstance() {
    return instance;
  }

  /**
   * Create a new event, i.e., wireformat object from the marshalled
   * bytes of said object.
   * 
   * @param message
   * @return the event object from the <code>byte[]</code>.
   * @throws IOException
   */
  public Event createEvent(byte[] marshalledBytes) throws IOException {

    switch ( ByteBuffer.wrap( marshalledBytes ).getInt() )
    {
      case Protocol.REGISTER_REQUEST :
        return new RegisterRequest( marshalledBytes );

      case Protocol.REGISTER_RESPONSE :
        return new RegisterResponse( marshalledBytes );

      case Protocol.UNREGISTER_REQUEST :
        return new RegisterRequest( marshalledBytes );

      case Protocol.MINOR_HEARTBEAT :
        return new MinorHeartbeat( marshalledBytes );

      case Protocol.WRITE_FILE_REQUEST :
        return new WriteFileRequest( marshalledBytes );

      case Protocol.WRITE_FILE_RESPONSE :
        return new WriteFileResponse( marshalledBytes );

      case Protocol.WRITE_CHUNK_REQUEST :
        return new WriteChunkRequest( marshalledBytes );

      case Protocol.LIST_FILE_REQUEST :
        return new ListFileRequest( marshalledBytes );

      case Protocol.LIST_FILE_RESPONSE :
        return new ListFileResponse( marshalledBytes );

      case Protocol.READ_FILE_REQUEST :
        return new ReadFileRequest( marshalledBytes );

      case Protocol.READ_FILE_RESPONSE :
        return new ReadFileResponse( marshalledBytes );

      case Protocol.READ_CHUNK_REQUEST :
        return new ReadChunkRequest( marshalledBytes );

      case Protocol.READ_CHUNK_RESPONSE :
        return new ReadChunkResponse( marshalledBytes );

      case Protocol.HEALTH_REQUEST :
        return new HealthRequest( marshalledBytes );

      default :
        LOG.error( "Event could not be created. "
            + ByteBuffer.wrap( marshalledBytes ).getInt() );
        return null;
    }
  }
}
