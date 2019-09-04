package cs555.system.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import cs555.system.node.Client;
import cs555.system.transport.TCPConnection;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.WriteChunks;
import cs555.system.wireformats.WriteQuery;

/**
 * 
 * @author stock
 *
 */
public class ClientSenderThread implements Runnable {

  private static final Logger LOG = new Logger();

  private List<File> files;

  private final Object lock = new Object();

  private Client node;

  private String[] routes;

  private boolean running = false;


  public ClientSenderThread(Client node) {
    this.node = node;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  public void unlock() {
    synchronized ( this.lock )
    {
      this.lock.notify();
    }
  }

  /**
   * Check if the thread is in a running state.
   * 
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return this.running;
  }

  /**
   * Set the files to process from the outbound directory.
   * 
   * @param files
   */
  public void setFiles(List<File> files) {
    this.files = files;
  }

  /**
   * Calling method checked for validity of routes;
   * 
   * @param routes
   */
  public void setRoutes(String[] routes) {
    this.routes = routes;
  }

  /**
   * Iterate through the files and send them to the chunk servers a
   * chunk at a time. A request to the controller will provide details
   * of which servers to communicate with.
   * 
   * @param files to send to the controller
   */
  @Override
  public void run() {
    running = true;

    final int numberOfFiles = files.size();

    synchronized ( lock )
    {
      for ( File file : files )
      {
        try ( InputStream is = new FileInputStream( file ) )
        {
          int numberOfChunks = ( int ) Math.ceil( file.length() / 1000.0 );
          byte[] request =
              ( new WriteQuery( file.getAbsolutePath(), numberOfChunks ) )
                  .getBytes();
          processIndividualFile( file, request, is );
        } catch ( IOException e )
        {
          LOG.error( "Unable to process the file " + file.getName() + ", "
              + e.getMessage() );
        } catch ( InterruptedException e )
        {
          Thread.currentThread().interrupt();
        }
      }
    }
    LOG.info( "Finished sending " + Integer.toString( numberOfFiles )
        + " file(s) to the controller.\n" );
    running = false;
    return;
  }

  /**
   * Process an individual file by traversing the chunks of bytes
   * within. This is accomplished via the following steps:
   * 
   * <ol>
   * <li>read the next chunk of the file</li>
   * <li>send request to controller for details of where to write the
   * chunk. wait for a reply from the controller - the client will
   * notify this thread</li>
   * <li>connect to the first item chunk server returned by the
   * controller, and send the data request</li>
   * </ol>
   * 
   * @param file to be processed
   * @param request to the controller for where to write the chunks
   * @param is input file stream
   * @throws IOException
   * @throws InterruptedException
   */
  public void processIndividualFile(File file, byte[] request, InputStream is)
      throws IOException, InterruptedException {

    String tmpName =
        ( new StringBuilder() ).append( File.separator ).append( "tmp" )
            .append( file.getAbsolutePath() ).append( "_chunk" ).toString();

    StringBuilder sb = new StringBuilder();
    int chunkNumber = 0;

    @SuppressWarnings( "unused" )
    int readBytes = 0;
    // TODO: Check if the byte[] needs cleared before reading last
    // chunk.
    byte[] chunk = new byte[ Protocol.CHUNK_SIZE ];
    while ( ( readBytes = is.read( chunk ) ) != -1 )
    {
      this.node.getControllerConnection().getTCPSender().sendData( request );
      lock.wait();
      if ( routes == null || routes.length == 0 )
      {
        throw new IOException( "There are no routes to send chunk too." );
      }
      LOG.debug( "routes: " + Arrays.toString( routes ) );
      String[] initialConnection = routes[ 0 ].split( ":" );
      TCPConnection connection = ConnectionUtilities.establishConnection( node,
          initialConnection[ 0 ], Integer.parseInt( initialConnection[ 1 ] ) );

      WriteChunks writeToChunkServer = new WriteChunks( sb.append( tmpName )
          .append( Integer.toString( chunkNumber++ ) ).toString(), chunk,
          routes );

      connection.getTCPSender().sendData( writeToChunkServer.getBytes() );
      sb.setLength( 0 );
      connection.close();
    }
  }

}
