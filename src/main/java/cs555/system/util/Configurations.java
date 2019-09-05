package cs555.system.util;

import java.util.Properties;

/**
 * Singleton class to load properties file for configuration.
 * 
 * This class is referenced from the StackOverflow post "Constants and
 * properties in java".
 * 
 * @author stock
 *
 */
public final class Configurations {

  public static Logger LOG = new Logger();

  private Properties properties = null;

  private static Configurations instance = null;

  /**
   * Private constructor
   * 
   */
  private Configurations() {
    this.properties = new Properties();
    try
    {
      properties.load( getClass().getClassLoader()
          .getResourceAsStream( Constants.CONF_NAME ) );
    } catch ( Exception e )
    {
      LOG.error( "Unable to load application properties. " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Creates the instance is synchronized to avoid multithreaded
   * problems.
   * 
   */
  private synchronized static void createInstance() {
    if ( instance == null )
    {
      instance = new Configurations();
    }
  }

  /**
   * Get the properties instance using a singleton pattern to guarantee
   * the creation of only one instance.
   * 
   * @return the instance associated with the properties file - creates
   *         a new instance if not previously created.
   */
  public static Configurations getInstance() {
    if ( instance == null )
    {
      createInstance();
    }
    return instance;
  }

  /**
   * 
   * @return a property of the property file denoted by the key
   */
  public String getProperty(String key) {
    String result = null;
    if ( key != null && !key.trim().isEmpty() )
    {
      result = this.properties.getProperty( key );
    }
    return result;
  }

  /**
   * Override the clone method to ensure the "unique instance"
   * requirement of this class.
   * 
   */
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }
}
