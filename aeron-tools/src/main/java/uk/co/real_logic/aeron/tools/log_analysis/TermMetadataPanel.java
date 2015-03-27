package uk.co.real_logic.aeron.tools.log_analysis;

import java.awt.Dimension;
import java.util.Observable;
import java.util.Observer;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.ParallelGroup;
import javax.swing.GroupLayout.SequentialGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;

import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class TermMetadataPanel extends JPanel implements Observer
{
    private JPanel northPanel = null;
    private JTabbedPane frameHeaderPane = null;
    private BufferVisualization vis = null;

    private JTextField initialTermIdFld = null;
    private JTextField activeTermIdFld = null;
    private JTextField termLengthFld = null;
    private JTextField[] dataOffsetFld = new JTextField[3];
    private JTextField[] flagsFld = new JTextField[3];
    private JTextField[] frameLengthFld = new JTextField[3];
    private JTextField[] headerTypeFld = new JTextField[3];
    private JTextField[] offsetFld = new JTextField[3];
    private JTextField[] sessionIdFld = new JTextField[3];
    private JTextField[] streamIdFld = new JTextField[3];
    private JTextField[] termIdFld = new JTextField[3];
    private JTextField[] termOffsetFld = new JTextField[3];
    private JTextField[] versionFld = new JTextField[3];

    private TermMetadataModel termModel = null;
    private LogModel model = null;

    public TermMetadataPanel(LogModel model)
    {
        this.model = model;
        init();
    }

    @Override
    public void update(Observable obs, Object obj)
    {

    }

    public void fillFields()
    {
        initialTermIdFld.setText(model.getInitialTermId() + "");
        activeTermIdFld.setText(model.getActiveTermId() + "");
        termLengthFld.setText(model.getTermLength() + "");

        for (int i = 0; i < 3; i++)
        {
            UnsafeBuffer defaultFrameHeader = model.getDefaultFrameHeader(i);
            DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

            dataHeaderFlyweight.wrap(defaultFrameHeader);
            dataOffsetFld[i].setText(dataHeaderFlyweight.dataOffset() + "");
            flagsFld[i].setText(dataHeaderFlyweight.flags() + "");
            frameLengthFld[i].setText(dataHeaderFlyweight.frameLength() + "");
            headerTypeFld[i].setText(dataHeaderFlyweight.headerType() + "");
            offsetFld[i].setText(dataHeaderFlyweight.offset() + "");
            sessionIdFld[i].setText(dataHeaderFlyweight.sessionId() + "");
            streamIdFld[i].setText(dataHeaderFlyweight.streamId() + "");
            termIdFld[i].setText(dataHeaderFlyweight.termId() + "");
            termOffsetFld[i].setText(dataHeaderFlyweight.termOffset() + "");
            versionFld[i].setText(dataHeaderFlyweight.version() + "");
        }
    }

    private void init()
    {
        northPanel = new JPanel();
        GroupLayout layout = new GroupLayout(northPanel);
        northPanel.setLayout(layout);
        //setLayout(new BorderLayout());
        add(northPanel/*, BorderLayout.NORTH*/);
        frameHeaderPane = new JTabbedPane();
        //frameHeaderPane.setPreferredSize(new Dimension(1100, 170));
        createFrameHeaderPanes();
        add(frameHeaderPane/*, BorderLayout.CENTER*/);
        vis = new BufferVisualization();
        vis.setPreferredSize(new Dimension(1000, 400));
        add(vis/*, BorderLayout.SOUTH*/);
        JLabel initialTermIdLbl = new JLabel("Initial Term Id:");
        initialTermIdFld = new JTextField("", 20);
        initialTermIdFld.setEditable(false);

        JLabel activeTermIdLbl = new JLabel("Active Term Id:");
        activeTermIdFld = new JTextField("", 20);
        activeTermIdFld.setEditable(false);

        JLabel termLengthLbl = new JLabel("Term Length:");
        termLengthFld = new JTextField("", 20);
        termLengthFld.setEditable(false);

        layout.setVerticalGroup(
                layout.createSequentialGroup()
                    .addGroup(layout.createParallelGroup(GroupLayout.Alignment.CENTER)
                            .addComponent(initialTermIdLbl)
                            .addComponent(initialTermIdFld)
                            .addComponent(activeTermIdLbl)
                            .addComponent(activeTermIdFld)
                            .addComponent(termLengthLbl)
                            .addComponent(termLengthFld)));

        layout.setHorizontalGroup(
                layout.createSequentialGroup()
                    .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                            .addComponent(initialTermIdLbl))
                    .addGroup(layout.createParallelGroup()
                            .addComponent(initialTermIdFld))
                    .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                            .addComponent(activeTermIdLbl))
                    .addGroup(layout.createParallelGroup()
                            .addComponent(activeTermIdFld))
                    .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                            .addComponent(termLengthLbl))
                    .addGroup(layout.createParallelGroup()
                            .addComponent(termLengthFld)));
    }

    private void createFrameHeaderPanes()
    {
        String[] labels = {
                "Data Offset:", "Flags:", "Frame Length:", "Header Type:",
                "Offset:", "Session Id:", "Stream Id:", "Term Id:", "Term Offset:",
                "Version:"
        };
        JPanel[] panels = new JPanel[3];
        JLabel[] dataOffsetLbls = new JLabel[3];
        JLabel[] flagsLbls = new JLabel[3];
        JLabel[] frameLengthLbls = new JLabel[3];
        JLabel[] headerTypeLbls = new JLabel[3];
        JLabel[] offsetLbls = new JLabel[3];
        JLabel[] sessionIdLbls = new JLabel[3];
        JLabel[] streamIdLbls = new JLabel[3];
        JLabel[] termIdLbls = new JLabel[3];
        JLabel[] termOffsetLbls = new JLabel[3];
        JLabel[] versionLbls = new JLabel[3];
        for (int i = 0; i < 3; i++)
        {
            panels[i] = new JPanel();
            GroupLayout layout = new GroupLayout(panels[i]);
            layout.setAutoCreateContainerGaps(true);
            panels[i].setLayout(layout);
            dataOffsetLbls[i] = new JLabel(labels[0]);
            dataOffsetFld[i] = new JTextField("", 10);
            dataOffsetFld[i].setEditable(false);
            flagsLbls[i] = new JLabel(labels[1]);
            flagsFld[i] = new JTextField("", 10);
            flagsFld[i].setEditable(false);
            frameLengthLbls[i] = new JLabel(labels[2]);
            frameLengthFld[i] = new JTextField("", 10);
            frameLengthFld[i].setEditable(false);
            headerTypeLbls[i] = new JLabel(labels[3]);
            headerTypeFld[i] = new JTextField("", 10);
            headerTypeFld[i].setEditable(false);
            offsetLbls[i] = new JLabel(labels[4]);
            offsetFld[i] = new JTextField("", 10);
            offsetFld[i].setEditable(false);
            sessionIdLbls[i] = new JLabel(labels[5]);
            sessionIdFld[i] = new JTextField("", 10);
            sessionIdFld[i].setEditable(false);
            streamIdLbls[i] = new JLabel(labels[6]);
            streamIdFld[i] = new JTextField("", 10);
            streamIdFld[i].setEditable(false);
            termIdLbls[i] = new JLabel(labels[7]);
            termIdFld[i] = new JTextField("", 10);
            termIdFld[i].setEditable(false);
            termOffsetLbls[i] = new JLabel(labels[8]);
            termOffsetFld[i] = new JTextField("", 10);
            termOffsetFld[i].setEditable(false);
            versionLbls[i] = new JLabel(labels[9]);
            versionFld[i] = new JTextField("", 10);
            versionFld[i].setEditable(false);
            SequentialGroup seqGroup = layout.createSequentialGroup();
            ParallelGroup group1 = layout.createParallelGroup();
            group1.addComponent(dataOffsetLbls[i]).addComponent(dataOffsetFld[i]);
            group1.addComponent(flagsLbls[i]).addComponent(flagsFld[i]);
            ParallelGroup group2 = layout.createParallelGroup();
            group2.addComponent(frameLengthLbls[i]).addComponent(frameLengthFld[i]);
            group2.addComponent(headerTypeLbls[i]).addComponent(headerTypeFld[i]);
            ParallelGroup group3 = layout.createParallelGroup();
            group3.addComponent(offsetLbls[i]).addComponent(offsetFld[i]);
            group3.addComponent(sessionIdLbls[i]).addComponent(sessionIdFld[i]);
            ParallelGroup group4 = layout.createParallelGroup();
            group4.addComponent(streamIdLbls[i]).addComponent(streamIdFld[i]);
            group4.addComponent(termIdLbls[i]).addComponent(termIdFld[i]);
            ParallelGroup group5 = layout.createParallelGroup();
            group5.addComponent(termOffsetLbls[i]).addComponent(termOffsetFld[i]);
            group5.addComponent(versionLbls[i]).addComponent(versionFld[i]);
            seqGroup.addGroup(group1);
            seqGroup.addGroup(group2);
            seqGroup.addGroup(group3);
            seqGroup.addGroup(group4);
            seqGroup.addGroup(group5);
            ParallelGroup pgroup1 = layout.createParallelGroup();
            pgroup1.addComponent(dataOffsetLbls[i]).addComponent(frameLengthLbls[i]);
            pgroup1.addComponent(offsetLbls[i]).addComponent(streamIdLbls[i]);
            pgroup1.addComponent(termOffsetLbls[i]);
            ParallelGroup pgroup2 = layout.createParallelGroup();
            pgroup2.addComponent(dataOffsetFld[i]).addComponent(frameLengthFld[i]);
            pgroup2.addComponent(offsetFld[i]).addComponent(streamIdFld[i]);
            pgroup2.addComponent(termOffsetFld[i]);
            ParallelGroup pgroup3 = layout.createParallelGroup();
            pgroup3.addComponent(flagsLbls[i]).addComponent(headerTypeLbls[i]);
            pgroup3.addComponent(sessionIdLbls[i]).addComponent(termIdLbls[i]);
            pgroup3.addComponent(versionLbls[i]);
            ParallelGroup pgroup4 = layout.createParallelGroup();
            pgroup4.addComponent(flagsFld[i]).addComponent(headerTypeFld[i]);
            pgroup4.addComponent(sessionIdFld[i]).addComponent(termIdFld[i]);
            pgroup4.addComponent(versionFld[i]);
            layout.setVerticalGroup(seqGroup);
            layout.setHorizontalGroup(layout.createSequentialGroup()
                    .addGroup(pgroup1).addGroup(pgroup2).addGroup(pgroup3).addGroup(pgroup4));
            frameHeaderPane.add(panels[i], "Frame Header " + (i + 1));
        }
    }
}
