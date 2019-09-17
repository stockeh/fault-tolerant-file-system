package cs555.system.util;

/**
 * Progress bar to monitor the status of reading or writing a file.
 * 
 * @author stock
 *
 */
public class ProgressBar {

  private StringBuilder sb;

  private String filename;

  private final static char[] WORKCHARS = { '|', '/', '-', '\\' };

  private final static String FORMAT = "\r%20s %3d%% %s %c";

  /**
   * Default constructor -
   * 
   * @param filename
   */
  public ProgressBar(String filename) {
    this.sb = new StringBuilder( 60 );
    this.filename = filename;
  }

  /**
   * Reset output stream
   */
  public void reset() {
    System.out.flush();
    System.out.println();
  }
  
  /**
   * Invoked when there is progress
   *
   * @param current representing work done so far
   * @param total representing the total work
   */
  public void update(int current, int total) {
    int percent = ( ++current * 100 ) / total;
    int extrachars = ( percent / 2 ) - sb.length();

    while ( extrachars-- > 0 )
    {
      sb.append( '#' );
    }
    System.out.printf( FORMAT, filename, percent, sb,
        WORKCHARS[ current % WORKCHARS.length ] );

    if ( current == total )
    {
      reset();
    }
  }
}
