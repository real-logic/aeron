package uk.co.real_logic.aeron.tools.log_analysis;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.ParallelGroup;
import javax.swing.GroupLayout.SequentialGroup;
import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

public class StatsPanel extends JPanel
{
    private LogModel model = null;
    private GroupLayout layout = null;
    private JLabel[] labels = null;
    private JTextField[] fields = null;
    private JToggleButton[] watchBtns = null;
    private ImageIcon watchIcon = null;
    private JPanel transportStats = null;
    private JPanel transports = null;
    private boolean running = true;
    JTextArea textArea = null;
    JScrollPane scrollPane = null;

    public StatsPanel(LogModel model)
    {
        this.model = model;
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("eye1.png").getFile());
        watchIcon = new ImageIcon(file.toString());
        init();
    }

    public void fillFields()
    {
        DriverStats stats = model.getStats();

        labels = new JLabel[stats.getNumLabels()];
        fields = new JTextField[stats.getNumLabels()];
        watchBtns = new JToggleButton[stats.getNumLabels()];

        for (int i = 0; i < stats.getNumLabels(); i++)
        {
            labels[i] = new JLabel(stats.getLabel(i));
            fields[i] = new JTextField(stats.getValue(i) + "", 20);
            fields[i].setEditable(false);
            watchBtns[i] = new JToggleButton(watchIcon);
            watchBtns[i].addActionListener(new ActionListener()
                {
                    public void actionPerformed(ActionEvent e)
                    {
                        JToggleButton src = (JToggleButton)e.getSource();

                        for (int i = 0; i < watchBtns.length; i++)
                        {
                            if (watchBtns[i] == src)
                            {
                                System.out.println("Button: " + i + " was pressed");
                            }
                        }
                    }
                }
            );
        }

        layout.setAutoCreateGaps(true);
        SequentialGroup seqGroup = layout.createSequentialGroup();

        seqGroup.addGroup(createParallelGroup(new int[] {0, 1}));
        seqGroup.addGroup(createParallelGroup(new int[] {2, 3, 4}));
        seqGroup.addGroup(createParallelGroup(new int[] {5, 6}));
        seqGroup.addGroup(createParallelGroup(new int[] {7, 8}));
        seqGroup.addGroup(createParallelGroup(new int[] {9, 10}));
        seqGroup.addGroup(createParallelGroup(new int[] {11, 12, 13}));
        seqGroup.addGroup(createParallelGroup(new int[] {14, 15}));
        seqGroup.addGroup(createParallelGroup(new int[] {16, 17, 18}));
        seqGroup.addGroup(createParallelGroup(new int[] {19, 20}));

        ParallelGroup pgroup1 = createParallelLabelsGroup(new int[] {0, 2, 5, 7, 9, 11, 14, 16, 19});
        ParallelGroup pgroup2 = createParallelFieldsGroup(new int[] {0, 2, 5, 7, 9, 11, 14, 16, 19});
        ParallelGroup pgroup3 = createParallelWatchGroup(new int[] {0, 2, 5, 7, 9, 11, 14, 16, 19});
        ParallelGroup pgroup4 = createParallelLabelsGroup(new int[] {1, 3, 6, 8, 10, 12, 15, 17, 20});
        ParallelGroup pgroup5 = createParallelFieldsGroup(new int[] {1, 3, 6, 8, 10, 12, 15, 17, 20});
        ParallelGroup pgroup6 = createParallelWatchGroup(new int[] {1, 3, 6, 8, 10, 12, 15, 17, 20});
        ParallelGroup pgroup7 = createParallelLabelsGroup(new int[] {4, 13, 18});
        ParallelGroup pgroup8 = createParallelFieldsGroup(new int[] {4, 13, 18});
        ParallelGroup pgroup9 = createParallelWatchGroup(new int[] {4, 13, 18});

        layout.setVerticalGroup(seqGroup);
        layout.setHorizontalGroup(layout.createSequentialGroup()
                .addGroup(pgroup1).addGroup(pgroup2).addGroup(pgroup3)
                .addGroup(pgroup4).addGroup(pgroup5).addGroup(pgroup6)
                .addGroup(pgroup7).addGroup(pgroup8).addGroup(pgroup9));

        StringBuffer buff = new StringBuffer();
        for (int i = 21; i < stats.getNumLabels(); i++)
        {
            buff.append(stats.getLabel(i) + ": " + stats.getValue(i) + "\n");
        }
        textArea.setText(buff.toString());

        Runnable task = new Runnable()
        {
            public void run()
            {
                while (StatsPanel.this.isVisible())
                {
                    updateFields();
                    try
                    {
                        Thread.sleep(1000);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        };
        Thread thread = new Thread(task);
        thread.start();
    }

    public void updateFields()
    {
        DriverStats stats = model.getStats();
        stats.populate();
        for (int i = 0; i < 21; i++)
        {
            fields[i].setText(stats.getValue(i) + "");
            fields[i].invalidate();
        }

        StringBuffer buff = new StringBuffer();
        for (int i = 21; i < stats.getNumLabels(); i++)
        {
            buff.append(stats.getLabel(i) + ": " + stats.getValue(i) + "\n");
        }
        textArea.setText(buff.toString());
        //this.repaint();
        this.validate();
    }

    public void shutdown()
    {
        running = false;
    }

    private void init()
    {
        transportStats = new JPanel();
        transports = new JPanel();
        layout = new GroupLayout(transportStats);
        transportStats.setLayout(layout);
        setLayout(new BorderLayout());

        textArea = new JTextArea();
        scrollPane = new JScrollPane(textArea);
        transports.add(scrollPane);

        setBorder(BorderFactory.createTitledBorder("  Driver Stats  "));

        add(transportStats, BorderLayout.NORTH);
        add(transports, BorderLayout.SOUTH);
    }

    private ParallelGroup createParallelGroup(int[] idx)
    {
        ParallelGroup group = layout.createParallelGroup(GroupLayout.Alignment.CENTER);

        for (int i = 0; i < idx.length; i++)
        {
            group.addComponent(labels[idx[i]]).addComponent(fields[idx[i]]).addComponent(watchBtns[idx[i]]);
        }

        return group;
    }

    private ParallelGroup createParallelLabelsGroup(int[] idx)
    {
        ParallelGroup group = layout.createParallelGroup(GroupLayout.Alignment.TRAILING);

        for (int i = 0; i < idx.length; i++)
        {
            group.addComponent(labels[idx[i]]);
        }

        return group;
    }

    private ParallelGroup createParallelFieldsGroup(int[] idx)
    {
        ParallelGroup group = layout.createParallelGroup();

        for (int i = 0; i < idx.length; i++)
        {
            group.addComponent(fields[idx[i]]);
        }

        return group;
    }

    private ParallelGroup createParallelWatchGroup(int[] idx)
    {
        ParallelGroup group = layout.createParallelGroup();

        for (int i = 0; i < idx.length; i++)
        {
            group.addComponent(watchBtns[idx[i]]);
        }

        return group;
    }
}
