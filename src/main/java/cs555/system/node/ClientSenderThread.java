package cs555.system.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.wireformats.WriteChunkRequest;
import cs555.system.wireformats.WriteFileRequest;

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

  /**
   * Default constructor -
   * 
   * @param node
   */
  protected ClientSenderThread(Client node) {
    this.node = node;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  protected void unlock() {
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
  protected boolean isRunning() {
    return this.running;
  }

  /**
   * Set the files to process from the outbound directory.
   * 
   * @param files
   */
  protected void setFiles(List<File> files) {
    this.files = files;
  }

  /**
   * Calling method checked for validity of routes;
   * 
   * @param routes
   */
  protected void setRoutes(String[] routes) {
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

    int numberOfFiles = files.size();

    synchronized ( lock )
    {
      for ( File file : files )
      {
        try ( InputStream is = new FileInputStream( file ) )
        {
          processIndividualFile( file, is );
        } catch ( IOException e )
        {
          LOG.error( "Unable to process the file " + file.getName() + ". "
              + e.getMessage() );
          --numberOfFiles;
        } catch ( InterruptedException e )
        {
          Thread.currentThread().interrupt();
        }
      }
    }
    if ( numberOfFiles > 0 )
    {
      LOG.info( "Finished sending " + Integer.toString( numberOfFiles )
          + " file(s) to the controller.\n" );
    }
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
   * @param is input file stream
   * @throws IOException
   * @throws InterruptedException
   */
  private void processIndividualFile(File file, InputStream is)
      throws IOException, InterruptedException {

    String name = file.getAbsolutePath();
    
    int filelength = ( int ) file.length();
    int numberOfChunks =
        ( int ) Math.ceil( ( double ) filelength / Constants.CHUNK_SIZE );
    
    int sequence = 0;

    int length = 0;
    byte[] chunk = new byte[ Constants.CHUNK_SIZE ];
    while ( ( length = is.read( chunk ) ) != -1 )
    {
      byte[] request = ( new WriteFileRequest( file.getAbsolutePath(), sequence,
          filelength, numberOfChunks ) ).getBytes();
      
      this.node.getControllerConnection().getTCPSender().sendData( request );
      // wait for response from controller containing routing information.
      lock.wait();
      if ( routes == null || routes.length == 0 )
      {
        throw new IOException( "There are no routes to send chunk too." );
      }
      LOG.debug( "routes: " + Arrays.toString( routes ) );

      String[] initialConnection = routes[ 0 ].split( ":" );
      TCPConnection connection = ConnectionUtilities.establishConnection( node,
          initialConnection[ 0 ], Integer.parseInt( initialConnection[ 1 ] ) );

      // Pad elements b[k] through b[b.length-1] with zeros
      Arrays.fill( chunk, length, Constants.CHUNK_SIZE, ( byte ) 0 );

      WriteChunkRequest writeToChunkServer =
          new WriteChunkRequest( name, sequence++, chunk, routes );

      connection.getTCPSender().sendData( writeToChunkServer.getBytes() );
      connection.close();
    }
  }

}
