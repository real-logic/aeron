package uk.co.real_logic.aeron.tools.log_analysis;

import java.io.File;
import java.util.Observable;
import java.util.Observer;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.layout.VBox;

public class NavigationPanel extends VBox implements Observer
{
    private TreeItem<String> rootItem = null;
    private TreeView<String> tree = null;

    private LogModel model = null;
    private NavigationModel navModel = null;

    public NavigationPanel(LogModel model)
    {
        this.model = model;
        navModel = new NavigationModel();
        navModel.addObserver(this);
        init();
    }

    public void update(Observable obs, Object obj)
    {
        rootItem.getChildren().removeAll(rootItem.getChildren());

        addChildren(navModel.getDirectory(), rootItem);
    }

    public NavigationModel getModel()
    {
        return navModel;
    }
    private void init()
    {
        rootItem = new TreeItem<String>(navModel.getTitle());
        rootItem.setExpanded(true);

        addChildren(navModel.getDirectory(), rootItem);

        tree = new TreeView<String>(rootItem);

        tree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<TreeItem<String>>()
        {
            public void changed(ObservableValue<? extends TreeItem<String>> observableValue,
                                TreeItem<String> oldItem, TreeItem<String> newItem)
            {
                navModel.setSelectedFile(newItem.getValue());
            }
        });
        getChildren().add(tree);
    }

    private void addChildren(String filename, TreeItem<String> parent)
    {
        File files = new File(filename);
        if (files.listFiles() != null)
        {
            for (int i = 0; i < files.listFiles().length; i++)
            {
                TreeItem<String> item = new TreeItem(files.list()[i]);
                item.setExpanded(true);
                parent.getChildren().add(item);
                if (files.listFiles()[i].isDirectory())
                {
                    addChildren(files.listFiles()[i].getAbsolutePath(), item);
                }
            }
        }
    }
}
