package dev.prpatel.iceberg.app;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Supplies the table's schema to the text-to-SQL prompt, read from the catalog rather than
 * hardcoded.
 *
 * The description of each column is not lost by reading it at runtime: the CREATE TABLE in
 * {@link dev.prpatel.iceberg.tools.IcebergService} already attaches every one as a column
 * {@code COMMENT}, so {@code DESCRIBE TABLE} hands them back. Iceberg keeps them in the catalog,
 * which makes the catalog the single source of truth - add a column and the model is told about it
 * on the next question, with no code change and no redeploy.
 *
 * The result is cached because it changes only when the table does. {@link #refresh()} drops the
 * cache, and the admin actions that can alter the table call it.
 */
@Component
public class SchemaStore {

    private final SparkSession spark;
    private final String table;

    /** Written once per refresh and read by request threads, hence volatile. */
    private volatile String cached;

    public SchemaStore(SparkSession spark,
                       @Value("${app.table.name:lakekeeper.housing.staging_prices}") String table) {
        this.spark = spark;
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    /** The schema block for the prompt. Reads the catalog on the first call after a refresh. */
    public String get() {
        String current = cached;
        if (current == null) {
            current = describe();
            cached = current;
        }
        return current;
    }

    /** Forget the cached schema; the next question reads the catalog again. */
    public void refresh() {
        cached = null;
    }

    /** True once the catalog has actually been read, so the admin page can say so. */
    public boolean isCached() {
        return cached != null;
    }

    private String describe() {
        try {
            List<Row> rows = spark.sql("DESCRIBE TABLE " + table).collectAsList();

            StringBuilder out = new StringBuilder("Schema: table {\n");
            int n = 0;
            for (Row row : rows) {
                String name = text(row, 0);
                String type = text(row, 1);
                String comment = text(row, 2);

                // DESCRIBE appends partitioning and metadata sections after the columns, separated
                // by a blank name or a "# ..." heading. Everything from there on is not a column.
                if (name.isEmpty() || name.startsWith("#")) {
                    break;
                }

                out.append("  ").append(++n).append(": ").append(name).append(": ").append(type);
                if (!comment.isEmpty()) {
                    out.append(" (").append(comment).append(')');
                }
                out.append('\n');
            }
            out.append("}\n");

            System.out.println("schema: read " + n + " columns from the catalog for " + table);
            return out.toString();
        } catch (Exception e) {
            // Before Setup Environment and Load Data the table does not exist yet. Say so in the
            // prompt rather than failing the request - the model then has a reason to explain
            // itself, instead of inventing columns against a schema it was never given.
            System.err.println("schema: could not describe " + table + " (" + e + ")");
            return "Schema: unavailable - the table " + table + " has not been created yet.\n";
        }
    }

    private static String text(Row row, int i) {
        Object value = row.isNullAt(i) ? null : row.get(i);
        return value == null ? "" : value.toString().trim();
    }
}
