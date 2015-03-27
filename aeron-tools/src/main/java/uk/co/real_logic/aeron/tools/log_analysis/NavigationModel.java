package uk.co.real_logic.aeron.tools.log_analysis;

import java.util.Observable;

/**
 * Created by philip on 3/27/15.
 */

public class NavigationModel extends Observable
{
    public static final String TITLE = "Aeron";
    public static final String DEFAULT_DIRECTORY = System.getProperty("java.io.tmpdir") + "/aeron";
    private String directory = DEFAULT_DIRECTORY;
    private String selectedFile= "";

    public NavigationModel()
    {

    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
        setChanged();
        notifyObservers();
    }

    public String getDirectory()
    {
        return directory;
    }

    public String getTitle()
    {
        return TITLE;
    }

    public void setSelectedFile(String selectedFile)
    {
        this.selectedFile = selectedFile;
    }

}
