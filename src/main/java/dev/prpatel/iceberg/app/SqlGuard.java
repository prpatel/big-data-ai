package dev.prpatel.iceberg.app;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Decides whether a piece of SQL is safe to run, and caps how much it can return.
 *
 * This matters more here than it does behind the web UI. A person typing a question is at least
 * reading what comes back; an agent calling {@code run_query} over MCP is not, and it will happily
 * try whatever it thinks the schema implies. The rule is deliberately narrow: one statement, and it
 * has to be a query.
 *
 * The check is a parse, not a regex. Spark's own parser decides what the text means, so
 * {@code "SELECT 1; DROP TABLE t"} fails to parse rather than sneaking a second statement past a
 * pattern, and comment tricks like {@code "SELECT/**"}{@code "/1"} cannot change the verdict.
 */
@Component
public class SqlGuard {

    /** A statement that passed the guard, with a row cap applied if it did not have one. */
    public record CheckedSql(String sql, boolean limitApplied) {}

    /** Thrown when the SQL is not something we are willing to execute. */
    public static class RejectedException extends RuntimeException {
        public RejectedException(String message) {
            super(message);
        }
    }

    private final SparkSession spark;
    private final int maxRows;

    public SqlGuard(SparkSession spark, @Value("${app.sql.max-rows:200}") int maxRows) {
        this.spark = spark;
        this.maxRows = maxRows;
    }

    public int maxRows() {
        return maxRows;
    }

    /**
     * Parse the statement, reject anything that is not a read, and add a LIMIT if it has none.
     *
     * @throws RejectedException if it does not parse, or parses to something that writes
     */
    public CheckedSql check(String sql) {
        return check(sql, maxRows);
    }

    /**
     * As {@link #check(String)}, but with the row cap the caller asked for.
     *
     * The cap is a real LIMIT in the plan, not an instruction in the prompt and not a truncation
     * when rendering: the model may ignore the first, and the second has already done the work.
     */
    public CheckedSql check(String sql, int rows) {
        int cap = rows > 0 ? Math.min(rows, maxRows) : maxRows;
        if (sql == null || sql.isBlank()) {
            throw new RejectedException("No SQL was provided.");
        }
        String trimmed = sql.strip().replaceAll(";\\s*$", "");

        LogicalPlan plan;
        try {
            plan = spark.sessionState().sqlParser().parsePlan(trimmed);
        } catch (Throwable t) {
            // Throwable rather than Exception on purpose. Multiple statements land here, as do
            // ordinary syntax errors - but so does `CALL <catalog>.system.<procedure>(...)`, which
            // makes Iceberg's extended parser reach for a Scala class that is not on this runtime's
            // classpath and throw NoClassDefFoundError. For a guard, "could not confidently parse
            // this as a query" and "will not run it" are the same answer, whatever was thrown.
            throw new RejectedException("That is not a single valid Spark SQL statement: " + t);
        }

        // Everything that changes state - INSERT, DELETE, DROP, CREATE, MERGE, CALL, ALTER, and the
        // Iceberg procedures - parses to a Command. Queries do not.
        if (plan instanceof Command) {
            throw new RejectedException(
                    "Only read queries are allowed here. That statement would modify the warehouse ("
                            + plan.getClass().getSimpleName() + ").");
        }
        // INSERT INTO is the one common write that is not a Command in Spark's tree.
        if (plan.getClass().getSimpleName().startsWith("InsertInto")) {
            throw new RejectedException("Only read queries are allowed here. That statement writes.");
        }

        if (plan instanceof GlobalLimit) {
            return new CheckedSql(trimmed, false);
        }
        // Wrapping rather than appending, so it still works for set operations and ORDER BY.
        return new CheckedSql("SELECT * FROM (" + trimmed + ") LIMIT " + cap, true);
    }
}
