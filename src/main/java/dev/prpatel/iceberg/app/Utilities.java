package dev.prpatel.iceberg.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class Utilities {

    /**
     * Manually fetches data and formats it into a table string, similar to show().
     *
     * @param dataset The Dataset to display.
     * @param numRows The number of rows to fetch and display.
     * @return A String containing the formatted table.
     */
    public static String formatDataSet(Dataset<Row> dataset, int numRows) {
        // 1. FETCH DATA
        String[] columns = dataset.columns();
        List<Row> rows = dataset.takeAsList(numRows);

        // 2. CALCULATE COLUMN WIDTHS
        int[] widths = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            widths[i] = columns[i].length();
        }

        for (Row row : rows) {
            for (int i = 0; i < columns.length; i++) {
                Object cell = row.get(i);
                String cellStr = (cell == null) ? "null" : cell.toString();
                if (cellStr.length() > widths[i]) {
                    widths[i] = cellStr.length();
                }
            }
        }

        // 3. BUILD THE STRING
        StringBuilder sb = new StringBuilder();

        // Create the top border
        appendBorder(sb, widths);

        // Create the header
        sb.append("|");
        for (int i = 0; i < columns.length; i++) {
            sb.append(String.format(" %-" + widths[i] + "s |", columns[i]));
        }
        sb.append("\n");

        // Create the header/content separator
        appendBorder(sb, widths);

        // Create the data rows
        for (Row row : rows) {
            sb.append("|");
            for (int i = 0; i < columns.length; i++) {
                Object cell = row.get(i);
                String cellStr = (cell == null) ? "null" : cell.toString();
                sb.append(String.format(" %-" + widths[i] + "s |", cellStr));
            }
            sb.append("\n");
        }

        // Create the bottom border
        appendBorder(sb, widths);

        return sb.toString();
    }

    private static void appendBorder(StringBuilder sb, int[] widths) {
        sb.append("+");
        for (int width : widths) {
            for (int i = 0; i < width + 2; i++) { // +2 for padding spaces
                sb.append("-");
            }
            sb.append("+");
        }
        sb.append("\n");
    }

    public static String formatForHtml(String text) {
        StringBuilder sb = new StringBuilder(text);
//        sb.insert(0, "<pre>");
//        sb.append("</pre>");
        return sb.toString();
    }

}
