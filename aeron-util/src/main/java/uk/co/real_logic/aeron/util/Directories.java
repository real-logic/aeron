package uk.co.real_logic.aeron.util;

/**
 * Location of directory configuration that is common between the client and the media driver.
 */
public class Directories
{
    /** Directory of the data buffers */
    public static final String DATA_DIR_PROPERTY_NAME = "aeron.data.dir";
    /** Default directory for data buffers */
    public static final String DATA_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/data";

    public static final String DATA_DIR = System.getProperty(DATA_DIR_PROPERTY_NAME, DATA_DIR_PROPERTY_NAME_DEFAULT);
}
