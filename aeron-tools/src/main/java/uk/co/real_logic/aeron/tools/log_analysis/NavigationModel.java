/*
 * Copyright 2015 Kaazing Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    private String selectedFile = "";

    public NavigationModel()
    {

    }

    public void setDirectory(final String directory)
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

    public void setSelectedFile(final String selectedFile)
    {
        this.selectedFile = selectedFile;
    }

}
