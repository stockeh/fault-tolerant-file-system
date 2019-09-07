package cs555.system.node;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.ReadChunkRequest;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.ReadFileResponse;

/**
 * 
 * @author stock
 *
 */
public class ClientReaderThread implements Runnable {

  private static final Logger LOG = new Logger();

  private final Object lock = new Object();

  private ReadChunkResponse readChunkResponse;

  private ReadFileResponse readFileResponse;

  private Client node;

  /**
   * Default constructor -
   * 
   * @param node
   * @param readFileResponse
   */
  protected ClientReaderThread(Client node, ReadFileResponse readFileResponse) {
    this.node = node;
    this.readFileResponse = readFileResponse;
  }

  /**
   * Set the ReadChunkResponse() from the chunk server
   * 
   * @param response
   */
  public void setReadChunkResponse(ReadChunkResponse response) {
    this.readChunkResponse = response;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  protected void unlock() {
    synchronized ( lock )
    {
      lock.notify();
    }
  }

  /**
   * 
   */
  @Override
  public void run() {
    String[][] chunks = readFileResponse.getChunks();

    byte[] request = null;
    try
    {
      request =
          new ReadChunkRequest( readFileResponse.getFilename() ).getBytes();
    } catch ( IOException e )
    {
      LOG.error( "Unable to convert ReadChunkRequest() message to byte array. "
          + e.getMessage() );
      e.printStackTrace();
      return;
    }
    byte[][] bytes = processIncomingChunks( chunks, request );
    if ( bytes != null )
    {
      int numberOfChunks = bytes.length;
      ByteBuffer fileBytes =
          ByteBuffer.allocate( numberOfChunks * bytes[ 0 ].length );
      for ( int i = 0; i < numberOfChunks; ++i )
      {
        fileBytes.put( bytes[ i ] );
      }
      try
      {
        writeFile( readFileResponse.getFilename(), fileBytes.array() );
      } catch ( IOException e )
      {
        LOG.error( "Unable to save file \'" + readFileResponse.getFilename()
            + "\' to disk." );
        e.printStackTrace();
      }
    }
  }

  /**
   * 
   * 
   * @param chunks
   * @param request
   * @return
   */
  private byte[][] processIncomingChunks(String[][] chunks, byte[] request) {
    byte[][] fileBytes = new byte[ chunks.length ][ Constants.CHUNK_SIZE ];
    int replication = 0;
    for ( int sequence = 0; sequence < chunks.length; )
    {
      String[] connectionDetails =
          chunks[ sequence ][ replication ].split( ":" );
      try
      {
        TCPConnection connection = ConnectionUtilities.establishConnection(
            node, connectionDetails[ 0 ],
            Integer.parseInt( connectionDetails[ 1 ] ) );
        connection.getTCPSender().sendData( request );
        synchronized ( this.lock )
        {
          lock.wait();
        }
        fileBytes[ readChunkResponse.getSequence() ] =
            readChunkResponse.getMessage();
        connection.close();
      } catch ( IOException | InterruptedException e )
      {
        LOG.error(
            "Unable to send ReadChunkRequest() message to chunk server \'"
                + chunks[ sequence ][ replication ]
                + "\' trying next replication if possible. " + e.getMessage() );

        if ( ++replication >= chunks[ 0 ].length )
        {
          LOG.error(
              "File is not readable beacause a given chunk can not be returned" );
          return null;
        }
        continue;
      }
      ++sequence;
    }
    return fileBytes;
  }

  /**
   * Save the chunk file bytes to disk.
   * 
   * @param filename
   * @param fileBytes
   * @throws IOException
   */
  private void writeFile(String filename, byte[] fileBytes) throws IOException {
    Path path = Paths.get( filename );
    Files.createDirectories( path.getParent() );
    Files.write( path, fileBytes );
  }
}
