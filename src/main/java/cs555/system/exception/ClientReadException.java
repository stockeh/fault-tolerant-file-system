package cs555.system.exception;

/**
 * Exception thrown when the client detects the a read failure from a
 * chunk server due to invalid content.
 * 
 * @author stock
 *
 */
public class ClientReadException extends Exception {

  /**
   * Generated serial version ID
   */
  private static final long serialVersionUID = -2405565377188157910L;

  /**
   * 
   * @param message
   */
  public ClientReadException(String message) {
    super( message );
  }

  /**
   * 
   * @param message
   * @param cause
   */
  public ClientReadException(String message, Throwable cause) {
    super( message, cause );
  }

  /**
   * 
   * @param cause
   */
  public ClientReadException(Throwable cause) {
    super( cause );
  }
}
