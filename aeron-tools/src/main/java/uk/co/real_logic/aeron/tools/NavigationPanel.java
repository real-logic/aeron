package uk.co.real_logic.aeron.tools;

import java.awt.Dimension;
import java.io.File;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreeSelectionModel;


public class NavigationPanel extends JPanel implements TreeSelectionListener
{
    private JTree fileList = null;
    private LogModel model = null;
    public NavigationPanel(LogModel model)
    {
        this.model = model;
        init();
    }

    private void init()
    {
        DefaultMutableTreeNode top = new DefaultMutableTreeNode("Aeron");
        String prefix = System.getProperty("java.io.tmpdir") + "/aeron";
        addChildren(prefix, top);

        fileList = new JTree(top);
        fileList.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        fileList.addTreeSelectionListener(this);

        JScrollPane listPane = new JScrollPane(fileList);
        listPane.setPreferredSize(new Dimension(250, 400));
        add(listPane);
    }

    private void addChildren(String filename, DefaultMutableTreeNode parent)
    {
        File files = new File(filename);
        System.out.println(filename);
        if (files.listFiles() == null)
        {
            return;
        }

        for (int i = 0; i < files.listFiles().length; i++)
        {
            DefaultMutableTreeNode node = new DefaultMutableTreeNode(files.list()[i]);
            parent.add(node);
            if (files.listFiles()[i].isDirectory())
            {
                addChildren(files.listFiles()[i].getAbsolutePath(), node);
            }
        }
    }

    @Override
    public void valueChanged(TreeSelectionEvent e)
    {
        boolean stats = false;
        DefaultMutableTreeNode node = (DefaultMutableTreeNode)fileList.getLastSelectedPathComponent();
        if (node.isLeaf())
        {
            TreeNode[] nodes = node.getPath();
            String path = System.getProperty("java.io.tmpdir") + "/aeron";
            if (node.toString().equalsIgnoreCase("cnc"))
            {
                stats = true;
            }
            else
            {
                stats = false;
            }
            for (int i = 1; i < nodes.length; i++)
            {
                path += "/" + nodes[i].toString();
            }

            if (stats)
            {
                model.processStatsBuffer();
            }
            else
            {
                model.processLogBuffer(path);
            }
        }
    }
}
