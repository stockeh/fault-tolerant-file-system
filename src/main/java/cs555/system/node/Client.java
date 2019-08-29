package cs555.system.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Scanner;
import cs555.system.transport.TCPConnection;
import cs555.system.util.Logger;
import cs555.system.wireformats.Event;
import cs555.system.wireformats.Protocol;
import cs555.system.wireformats.Register;

public class Client implements Node {

  public static Logger LOG = new Logger();

  private TCPConnection registryConnection;

  private static final String EXIT = "exit";

  private static final String HELP = "help";

  private String nodeHost;

  private int nodePort;

  /**
   * Default constructor - creates a new messaging node tying the
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
    if ( args.length < 2 )
    {
      LOG.error(
          "USAGE: java cs555.system.node.Client registry-host registry-port" );
      System.exit( 1 );
    }
    LOG.info( "Client Node starting up at: " + new Date() );
    try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
    {
      int nodePort = serverSocket.getLocalPort();
      Client node =
          new Client( InetAddress.getLocalHost().getHostName(), nodePort );
      node.registerClient( args[ 0 ], Integer.valueOf( args[ 1 ] ) );
      node.interact();
    } catch ( IOException e )
    {
      LOG.error( "Exiting " + e.getMessage() );
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

        case EXIT :
          deregisterClient();
          running = false;
          break;

        case HELP :
          System.out.println(
              "\n\tprint-shortest-path\t: print shortest path from this node to all others.\n\n"
                  + "\texit-overlay\t\t: leave the overlay prior to starting.\n" );
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
   * Remove the client node from the registry.
   * 
   */
  private void deregisterClient() {
    Register register = new Register( Protocol.DEREGISTER_REQUEST,
        this.nodeHost, this.nodePort );

    try
    {
      registryConnection.getTCPSenderThread().sendData( register.getBytes() );
      registryConnection.close();
    } catch ( IOException | InterruptedException e )
    {
      LOG.error( e.getMessage() );
      e.printStackTrace();
    }
  }

  /**
   * Registers a node with the registry.
   *
   * @param host identifier for the registry node.
   * @param port number for the registry node
   */
  private void registerClient(String registryHost, Integer registryPort) {
    try
    {
      Socket socketToTheServer = new Socket( registryHost, registryPort );
      TCPConnection connection = new TCPConnection( this, socketToTheServer );

      Register register = new Register( Protocol.REGISTER_REQUEST,
          this.nodeHost, this.nodePort );

      LOG.info(
          "Client Identifier: " + this.nodeHost + ":" + this.nodePort );
      connection.getTCPSenderThread().sendData( register.getBytes() );
      connection.start();

      this.registryConnection = connection;
    } catch ( IOException | InterruptedException e )
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

}
