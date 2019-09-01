package cs555.system.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;

/**
 * Single client to communicate with the file systems controller and
 * chunk servers for writing and reading data.
 * 
 * @author stock
 *
 */
public class Client implements Node {

  public static Logger LOG = new Logger();

  private TCPConnection controllerConnection;

  private String outboundDirectory;

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private static final String UPLOAD = "upload";

  private static final String LIST_FILES = "list-files";

  private String nodeHost;

  private int nodePort;

  /**
   * Default constructor - creates a new chunk server tying the
   * <b>host:port</b> combination for the node as the identifier for
   * itself.
   * 
   * @param nodeHost
   * @param nodePort
   */
  private Client(String nodeHost, int nodePort) {
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
  }

  /**
   * Initialize the client with the Controller.
   *
   * @param args
   */
  public static void main(String[] args) {
    if ( args.length < 3 )
    {
      LOG.error(
          "USAGE: java cs555.system.node.Client controller-host controller-port outbound-dir" );
      System.exit( 1 );
    }
    LOG.info( "Client Node starting up at: " + new Date() );

    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      int nodePort = serverSocket.getLocalPort();
      Client node =
          new Client( InetAddress.getLocalHost().getHostName(), nodePort );
      node.registerClient( args[ 0 ], Integer.valueOf( args[ 1 ] ) );
      node.outboundDirectory = args[ 2 ];
      node.interact();
    } catch ( IOException e )
    {
      LOG.error(
          "Unable to successfully start client. Exiting. " + e.getMessage() );
      e.printStackTrace();
    }

  }

  private void interact() {
    System.out.println(
        "\nInput a command to interact with processes. Input 'help' for a list of commands.\n" );
    boolean running = true;
    while ( running )
    {
      @SuppressWarnings( "resource" )
      Scanner scan = new Scanner( System.in );
      switch ( scan.nextLine().toLowerCase() )
      {

        case UPLOAD :
          try
          {
            uploadFiles();
          } catch ( IOException e )
          {
            LOG.error(
                "Failed to read from outbound directory - check arguments. "
                    + e.getMessage() );
            deregisterClient();
            running = false;
          }
          break;

        case LIST_FILES :
          listControllerFiles();
          break;

        case EXIT :
          deregisterClient();
          running = false;
          break;

        case HELP :
          displayHelp();
          break;

        default :
          LOG.error(
              "Unable to process. Please enter a valid command! Input 'help' for options." );
          break;
      }
    }
    LOG.info(
        nodeHost + ":" + nodePort + " has deregistered and is terminating." );
    System.exit( 0 );
  }

  /**
   * Grab outbound files to send to the controller. Return if there are
   * no files to upload.
   * 
   * @throws IOException if there are issues reading the outbound
   *         directory.
   */
  private void uploadFiles() throws IOException {
    List<File> files;
    try ( Stream<Path> paths = Files.walk( Paths.get( outboundDirectory ) ) )
    {
      files = paths.filter( Files::isRegularFile ).map( Path::toFile )
          .collect( Collectors.toList() );
    }
    if ( files == null || files.isEmpty() )
    {
      LOG.info( "There are no files to upload in " + outboundDirectory );
      return;
    }
    sender( files );
  }

  /**
   * Iterate through the files and send them to the chunk servers a
   * chunk at a time. A request to the controller will provide details
   * of which servers to communicate with.
   * 
   * @param files to send to the controller
   */
  private void sender(List<File> files) {
    byte[] b = new byte[ Protocol.CHUNK_SIZE ];
    for ( File file : files )
    {
      try ( InputStream is = new FileInputStream( file ) )
      {
        int readBytes = 0;
        while ( ( readBytes = is.read( b ) ) != -1 )
        {
          LOG.info( Integer.toString( readBytes ) );
        }
      } catch ( FileNotFoundException e )
      {
        e.printStackTrace();
      } catch ( IOException e )
      {
        e.printStackTrace();
      }
    }
  }

  /**
   * Send a request to the controller to display all files that are
   * stored on the chunk servers.
   * 
   */
  private void listControllerFiles() {

  }

  /**
   * Remove the client node from the controller.
   * 
   */
  private void deregisterClient() {
    Register register = new Register( Protocol.DEREGISTER_REQUEST,
        Protocol.CLIENT_ID, this.nodeHost, this.nodePort );

    try
    {
      controllerConnection.getTCPSender().sendData( register.getBytes() );
      controllerConnection.close();
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Registers a node with the controller.
   *
   * @param host identifier for the controller node.
   * @param port number for the controller node
   */
  private void registerClient(String controllerHost, Integer controllerPort) {
    try
    {
      Socket socketToTheServer = new Socket( controllerHost, controllerPort );
      TCPConnection connection = new TCPConnection( this, socketToTheServer );

      Register register = new Register( Protocol.REGISTER_REQUEST,
          Protocol.CLIENT_ID, this.nodeHost, this.nodePort );

      LOG.info( "Client Identifier: " + this.nodeHost + ":" + this.nodePort );
      connection.getTCPSender().sendData( register.getBytes() );
      connection.start();

      this.controllerConnection = connection;
    } catch ( IOException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

  @Override
  public void onEvent(Event event, TCPConnection connection) {
    LOG.debug( event.toString() );
    switch ( event.getType() )
    {

    }
  }

  private void displayHelp() {
    System.out.println( "\n\t" + EXIT
        + "\t: disconnect from the controller and terminate.\n\n\t" + UPLOAD
        + "\t: upload all files in " + outboundDirectory + "\n\n\t" + LIST_FILES
        + "\t: list readable files stored on the chunk servers.\n" );
  }

}
