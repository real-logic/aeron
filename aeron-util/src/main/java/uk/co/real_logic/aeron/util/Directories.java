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

    /** Directory of the admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME = "aeron.admin.dir";
    /** Default directory for admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/admin";

    public static final String DATA_DIR = System.getProperty(DATA_DIR_PROPERTY_NAME, DATA_DIR_PROPERTY_NAME_DEFAULT);
    public static final String ADMIN_DIR = System.getProperty(ADMIN_DIR_PROPERTY_NAME, ADMIN_DIR_PROPERTY_NAME_DEFAULT);
}
