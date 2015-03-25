package uk.co.real_logic.aeron.tools;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.Observable;
import java.util.Observer;

import javax.swing.*;

public class LogAnalyzer extends JFrame implements Observer
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
		setLayout(new BorderLayout());
		getContentPane().add(navigationPanel, BorderLayout.WEST);
		getContentPane().add(centerPanel, BorderLayout.CENTER);

		setLocation(0, 0);
		setSize(1400, 768);
		setVisible(true);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}

	private void createMenuBar()
	{
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
	}
	public void update(Observable o, Object arg)
  	{
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
		//this.invalidate();
		this.repaint();
		this.validate();
	}

	public static void main(String[] args)
	{
		new LogAnalyzer();
	}

}
