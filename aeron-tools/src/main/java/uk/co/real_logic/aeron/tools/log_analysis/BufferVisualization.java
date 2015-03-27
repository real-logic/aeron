package uk.co.real_logic.aeron.tools.log_analysis;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;

import javax.swing.JPanel;

public class BufferVisualization extends JPanel
{
    public BufferVisualization()
    {
        setSize(new Dimension(300, 300));
    }

    @Override
    public void paintComponent(Graphics g)
    {
        g.setColor(Color.black);
        g.fillRect(0, 0, getWidth(), getHeight());
    }
}
