package uk.co.real_logic.aeron.tools;

import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.Scene;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

import java.io.File;
import java.util.Observable;
import java.util.Observer;

import javax.swing.*;

public class LogAnalyzer extends Application implements Observer
{
	private JMenuBar menuBar = null;
	private LogModel model = null;
	private TermMetadataPanel termMetadataPanel = null;
	private NavigationPanel navigationPanel = null;
	private StatsPanel statsPanel = null;
	private JTextField initialTermId;
	private JTextField activeTermId;
	private JTextField termLength;
	private JPanel centerPanel = null;
	private Thread updateThread = null;

	public LogAnalyzer()
	{
		model = new LogModel();
		model.addObserver(this);

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

	public void start(Stage primaryStage)
	{
		primaryStage.setTitle("Log Analysis");

		TreeItem<String> rootItem = new TreeItem<String>("Aeron");
		rootItem.setExpanded(true);
		String prefix = System.getProperty("java.io.tmpdir") + "/aeron";

		addChildren(prefix, rootItem);

		TreeView<String> tree = new TreeView<String>(rootItem);
		StackPane root = new StackPane();
		root.getChildren().add(tree);
		tree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<TreeItem<String>>()
		{
			public void changed(ObservableValue<? extends TreeItem<String>> observableValue,
								TreeItem<String> oldItem, TreeItem<String> newItem)
			{
				System.out.println(newItem.getValue() + " was selected");
			}
		});
		primaryStage.setScene(new Scene(root, 400, 400));
		primaryStage.show();
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

	private void createMenuBar()
	{/*
		final JFileChooser fc = new JFileChooser(System.getProperty("java.io.tmpdir") + "aeron/");

		menuBar = new JMenuBar();
		JMenu fileMenu = new JMenu("File");
		JMenuItem openItem = new JMenuItem("Open");
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
	public void update(Observable o, Object arg)
  	{/*
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

	public static void main(String[] args)
	{
		launch(args);
	}

}
