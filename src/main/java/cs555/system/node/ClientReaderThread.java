package cs555.system.node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.ReadChunkRequest;
import cs555.system.wireformats.ReadChunkResponse;
import cs555.system.wireformats.ReadFileResponse;

/**
 * 
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
   * Continuously send requests to the chunk servers for chunk data.
   * Writing to disk if all chunks are successfully returned.
   */
  @Override
  public void run() {
    String[][] chunks = readFileResponse.getChunks();
    byte[][] bytes = processIncomingChunks( chunks );
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
   * Iterate over the connection details from the controller, requesting
   * chunk data from each chunk server.
   * 
   * @param chunks containing the chunk server connection details for
   *        each chunk in the file
   * @return a 2-dimensional array of chunk bytes for the given server
   */
  private byte[][] processIncomingChunks(String[][] chunks) {
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
        connection.start();
        byte[] request =
            new ReadChunkRequest( readFileResponse.getFilename(), sequence )
                .getBytes();
        connection.getTCPSender().sendData( request );
        synchronized ( lock )
        {
          lock.wait();
        }
        LOG.debug( "sequence: " + sequence );
        fileBytes[ sequence ] = readChunkResponse.getMessage();
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
   * Save the file to disk.
   * 
   * @param filename to write back to disk
   * @param fileBytes contain the content of the file
   * @throws IOException
   */
  private void writeFile(String filename, byte[] fileBytes) throws IOException {
    Timestamp timestamp = new Timestamp( System.currentTimeMillis() );
    String updatedFilename = filename + "_" + timestamp.toInstant();
    Path path = Paths.get( updatedFilename );
    Files.createDirectories( path.getParent() );
    Files.write( path, fileBytes );
    LOG.info(
        "Finished writing the file \'" + updatedFilename + "\' to disk." );
  }
}
