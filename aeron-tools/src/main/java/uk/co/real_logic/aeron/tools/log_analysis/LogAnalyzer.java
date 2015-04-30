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

import java.io.File;
import java.util.Observable;
import java.util.Observer;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TreeItem;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import javax.swing.JPanel;
import javax.swing.JTextField;

public class LogAnalyzer extends Application implements Observer
{
    private MenuBar menuBar = null;
    private LogModel model = null;
    private final TermMetadataPanel termMetadataPanel = null;
    private NavigationPanel navigationPanel = null;
    private final StatsPanel statsPanel = null;
    private JTextField initialTermId;
    private JTextField activeTermId;
    private JTextField termLength;
    private final JPanel centerPanel = null;
    private final Thread updateThread = null;
    private Scene scene = null;
    private Stage stage = null;

    public LogAnalyzer()
    {
        model = new LogModel();
        //model.addObserver(this);
/*
        UIManager.LookAndFeelInfo lafInfo[] = UIManager.getInstalledLookAndFeels();
        for (int i = 0; i < lafInfo.length; i++)
        {
            if (lafInfo[i].getName().equals("Nimbus"))
            {
                lafInfo[i] = new UIManager.LookAndFeelInfo("Nimbus",
                        "com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
                break;
            }
        }
        UIManager.setInstalledLookAndFeels(lafInfo);
        try
        {
            UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        createMenuBar();
        termMetadataPanel = new TermMetadataPanel(model);
        navigationPanel = new NavigationPanel(model);
        statsPanel = new StatsPanel(model);
        centerPanel = new JPanel();
        //createMetaDataPanel();
        /*
        setLayout(new BorderLayout());
        getContentPane().add(navigationPanel, BorderLayout.WEST);
        getContentPane().add(centerPanel, BorderLayout.CENTER);

        setLocation(0, 0);
        setSize(1400, 768);
        setVisible(true);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        */
    }

    @Override
    public void start(final Stage primaryStage)
    {
        stage = primaryStage;
        stage.setTitle("Log Analysis");
        scene = new Scene(new VBox(), 400, 400);
        scene.setFill(Color.CHOCOLATE);

        createMenuBar();
        navigationPanel = new NavigationPanel(model);

        ((VBox)scene.getRoot()).getChildren().addAll(navigationPanel);

        stage.setScene(scene);
        stage.show();
    }

    private void addChildren(final String filename, final TreeItem<String> parent)
    {
        final File files = new File(filename);
        if (files.listFiles() != null)
        {
            for (int i = 0; i < files.listFiles().length; i++)
            {
                final TreeItem<String> item = new TreeItem(files.list()[i]);
                item.setExpanded(true);
                parent.getChildren().add(item);
                if (files.listFiles()[i].isDirectory())
                {
                    addChildren(files.listFiles()[i].getAbsolutePath(), item);
                }
            }
        }
    }

    private void createMenuBar()
    {
        final DirectoryChooser fc = new DirectoryChooser();

        menuBar = new MenuBar();
        final Menu fileMenu = new Menu("File");
        final MenuItem openItem = new MenuItem("Open");
        openItem.setOnAction(new EventHandler<ActionEvent>()
        {
            @Override
            public void handle(final ActionEvent e)
            {
                final File file = fc.showDialog(stage);

                if (file != null)
                {
                    navigationPanel.getModel().setDirectory(file.getAbsolutePath());
                }
            }
        });
        fileMenu.getItems().add(openItem);
        menuBar.getMenus().add(fileMenu);
        ((VBox)scene.getRoot()).getChildren().addAll(menuBar);
    /*
        openItem.addActionListener(new ActionListener()
        {
            public void actionPerformed(ActionEvent e)
            {
                int retVal = fc.showOpenDialog(LogAnalyzer.this);
                if (retVal == JFileChooser.APPROVE_OPTION)
                {
                    File file = fc.getSelectedFile();
                    model.setFile(file);
                }
            }
        });
        fileMenu.add(openItem);
        menuBar.add(fileMenu);
        setJMenuBar(menuBar);
        */
    }
    @Override
    public void update(final Observable o, final Object arg)
    { /*
        getContentPane().remove(centerPanel);
        if (model.getLogType() == 1)
        {
            centerPanel = statsPanel;
            statsPanel.fillFields();
        }
        else if (model.getLogType() == 2)
        {
            centerPanel = termMetadataPanel;
            termMetadataPanel.fillFields();
        }
        getContentPane().add(centerPanel, BorderLayout.CENTER);
        this.repaint();
        this.validate();
        */
    }

    public static void main(final String[] args)
    {
        launch(args);
    }

}
