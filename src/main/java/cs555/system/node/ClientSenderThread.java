package cs555.system.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import cs555.system.exception.ClientWriteException;
import cs555.system.transport.TCPConnection;
import cs555.system.util.ConnectionUtilities;
import cs555.system.util.Constants;
import cs555.system.util.Logger;
import cs555.system.util.ProgressBar;
import cs555.system.util.Properties;
import cs555.system.util.ReedSolomonUtilities;
import cs555.system.wireformats.WriteChunkRequest;
import cs555.system.wireformats.WriteFileRequest;
import cs555.system.wireformats.WriteFileResponse;

/**
 * Thread that is created to get chunk data at every read request from
 * the client.
 * 
 * @author stock
 *
 */
public class ClientSenderThread implements Runnable {

  private static final Logger LOG = Logger.getInstance();

  private final Object lock;

  private List<File> files;

  private final Client node;

  private String[][] routes;

  private AtomicInteger totalReceived;

  private boolean ableToWrite;

  private boolean running;

  /**
   * Default constructor -
   * 
   * @param node
   */
  protected ClientSenderThread(Client node) {
    this.lock = new Object();
    this.totalReceived = new AtomicInteger( 0 );
    this.ableToWrite = true;
    this.running = false;
    this.node = node;
  }

  /**
   * Wake the sender thread upon receiving routing information for a
   * given chunk.
   * 
   */
  protected void unlock() {
    totalReceived.set( 0 );
    synchronized ( lock )
    {
      lock.notify();
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
   * @param response
   */
  protected void setRoutes(WriteFileResponse response) {
    routes[ response.getSequence() ] = response.getRoutingPath();
    if ( totalReceived.incrementAndGet() == routes.length )
    {
      unlock();
    }
  }

  /**
   * 
   * @param ableToUpdate
   */
  protected void setAbleToWrite(boolean ableToWrite) {
    this.ableToWrite = ableToWrite;
    if ( !this.ableToWrite )
    {
      unlock();
    }
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

    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );

    int numberOfFiles = files.size();
    LOG.info( "Started uploading " + numberOfFiles + " file(s) at "
        + sdf.format( System.currentTimeMillis() ) );
    ConnectionUtilities connections = new ConnectionUtilities();

    for ( File file : files )
    {
      try ( InputStream is = new FileInputStream( file ) )
      {
        processIndividualFile( file, is, connections );
      } catch ( IOException | ClientWriteException e )
      {
        LOG.error( "Unable to process the file " + file.getName() + ". "
            + e.getMessage() );
        ableToWrite = true;
        --numberOfFiles;
      } catch ( InterruptedException e )
      {
        Thread.currentThread().interrupt();
      }
    }
    LOG.info( "Finished uploading " + numberOfFiles + " file(s) at "
        + sdf.format( System.currentTimeMillis() ) + "\n" );
    connections.setAbleToClear( true );
    connections.closeCachedConnections();
    running = false;
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
   * @param connections utilities to cache connections
   * @throws IOException
   * @throws InterruptedException
   */
  private void processIndividualFile(File file, InputStream is,
      ConnectionUtilities connections)
      throws IOException, InterruptedException, ClientWriteException {

    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
    LOG.debug( "The file: " + file.getAbsolutePath() + " was last modified at "
        + sdf.format( file.lastModified() ) );

    int filelength = ( int ) file.length();
    int numberOfChunks =
        ( int ) Math.ceil( ( double ) filelength / Constants.CHUNK_SIZE );

    routes = new String[ numberOfChunks ][];

    WriteFileRequest request = new WriteFileRequest( file.getAbsolutePath(), 0,
        filelength, numberOfChunks );
    for ( int sequence = 0; sequence < numberOfChunks; ++sequence )
    {
      request.setSequence( sequence );
      this.node.getControllerConnection().getTCPSender()
          .sendData( request.getBytes() );
    }
    // wait for response from controller containing routing information.
    synchronized ( lock )
    {
      lock.wait();
    }
    if ( !ableToWrite )
    {
      throw new ClientWriteException( "The controller has not"
          + " received file chunk locations for the original file yet." );
    }
    sendWriteChunkRequest( file, is, connections, numberOfChunks );
  }

  /**
   * Send the individual chunks to only the initial destination for each
   * chunk / fragment.
   * 
   * The metadata is set with temporary version number ( the server will
   * detect the difference and update if necessary )
   * 
   * @param file
   * @param is
   * @param connections
   * @param numberOfChunks
   * @throws NumberFormatException
   * @throws IOException
   */
  private void sendWriteChunkRequest(File file, InputStream is,
      ConnectionUtilities connections, int numberOfChunks)
      throws NumberFormatException, IOException {
    byte[] message = new byte[ Constants.CHUNK_SIZE ];

    ProgressBar progress = new ProgressBar( file.getName() );

    WriteChunkRequest request = new WriteChunkRequest( file.getAbsolutePath(),
        0, null, file.lastModified(), 1, null );

    int sequence = 0, length = 0;
    while ( ( length = is.read( message ) ) != -1 )
    {
      // Only send to the first connection, whom will forward the rest
      String[] initialConnection = routes[ sequence ][ 0 ].split( ":" );

      TCPConnection connection =
          connections.cacheConnection( node, initialConnection, false );
      // Pad elements b[k] through b[b.length-1] with zeros
      Arrays.fill( message, length, Constants.CHUNK_SIZE, ( byte ) 0 );
      byte[][] messageToSend = new byte[][] { message };
      if ( Properties.SYSTEM_DESIGN_SCHEMA
          .equals( Constants.SYSTEM_TYPE_ERASURE ) )
      {
        messageToSend = ReedSolomonUtilities.encode( message );
      }
      request.setMessage( messageToSend );
      request.setRoutes( routes[ sequence ] );
      request.setSequence( sequence );

      connection.getTCPSender().sendData( request.getBytes() );
      progress.update( sequence, numberOfChunks );
      ++sequence;
    }
  }

}
