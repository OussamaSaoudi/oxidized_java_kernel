import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;

import javax.swing.*;
import java.awt.*;

public class Grapher {

    /**
     * Creates and displays a histogram comparing the two sets of measurements
     */
    public static void createHistogram(DescriptiveStatistics rustStats, DescriptiveStatistics javaStats) {
        // Convert statistics to double arrays
        double[] data1 = rustStats.getValues();
        double[] data2 = javaStats.getValues();

        // Create dataset for histogram
        HistogramDataset dataset = new HistogramDataset();
        dataset.setType(HistogramType.FREQUENCY);

        // Find the min and max values across both datasets to determine bin range
        double min = Math.min(rustStats.getMin(), javaStats.getMin());
        double max = Math.max(rustStats.getMax(), javaStats.getMax());

        // Add data to the dataset (with 20 bins)
        int bins = 20;
        dataset.addSeries("Rust", data1, bins, min, max);
        dataset.addSeries("Java", data2, bins, min, max);

        // Create the chart
        JFreeChart chart = ChartFactory.createHistogram(
                "Performance Comparison",
                "Execution Time (ms)",
                "Frequency",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // Customize appearance
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setRangeGridlinePaint(Color.GRAY);
        plot.setDomainGridlinePaint(Color.GRAY);

        XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
        renderer.setSeriesPaint(0, new Color(0, 120, 200, 150)); // Function 1 color
        renderer.setSeriesPaint(1, new Color(200, 80, 80, 150));  // Function 2 color
        renderer.setShadowVisible(false);
        renderer.setBarPainter(new org.jfree.chart.renderer.xy.StandardXYBarPainter());

        // Display the chart
        JFrame frame = new JFrame("Performance Comparison");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 600);

        ChartPanel chartPanel = new ChartPanel(chart);
        frame.setContentPane(chartPanel);
        frame.setVisible(true);
    }
}
