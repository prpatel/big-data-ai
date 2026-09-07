package dev.prpatel.iceberg.app;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * How a parsed-but-unresolved table reference renders, e.g.
     * {@code 'UnresolvedRelation [lakekeeper, housing, staging_prices], [], false}.
     *
     * Read from the plan's own rendering rather than by walking the tree: getting at the
     * identifiers in Java means converting a Scala Seq, and Scala interop is exactly what already
     * throws NoClassDefFoundError in the catch below. The string form is produced by Spark from
     * the same tree, and it cannot be spoofed from the SQL - a table literally named
     * "UnresolvedRelation [x" will not parse.
     */
    private static final Pattern RELATION = Pattern.compile("UnresolvedRelation \\[([^\\]]+)\\]");

    /**
     * Names a statement defines for itself, which are not tables at all.
     *
     * A CTE renders its aliases at the top of the plan as {@code CTE [x, y]}, and every reference
     * to one then appears as an ordinary {@code UnresolvedRelation [x]} lower down - identical in
     * the tree to a real table. Without this, {@code WITH x AS (SELECT … FROM housing…) SELECT *
     * FROM x} is refused for reading a table called "x" that was never anywhere but this query.
     */
    private static final Pattern CTE_NAMES = Pattern.compile("CTE \\[([^\\]]+)\\]");

    private final SparkSession spark;
    private final int maxRows;
    private final String allowedNamespace;

    public SqlGuard(SparkSession spark,
                    @Value("${app.sql.max-rows:200}") int maxRows,
                    @Value("${app.sql.allowed-namespace:lakekeeper.housing}") String allowedNamespace) {
        this.spark = spark;
        this.maxRows = maxRows;
        this.allowedNamespace = allowedNamespace == null ? "" : allowedNamespace.trim();
    }

    /**
     * Every table the statement actually reads, as dotted names - excluding names the statement
     * defined for itself.
     */
    static List<String> tablesIn(LogicalPlan plan) {
        String rendered = plan.toString();

        Set<String> defined = new HashSet<>();
        Matcher cte = CTE_NAMES.matcher(rendered);
        while (cte.find()) {
            for (String name : cte.group(1).split(",")) {
                defined.add(name.trim());
            }
        }

        List<String> found = new ArrayList<>();
        Matcher m = RELATION.matcher(rendered);
        while (m.find()) {
            String name = m.group(1).replace(", ", ".").trim();
            if (!defined.contains(name)) {
                found.add(name);
            }
        }
        return found;
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

        // Reads are allowed, but not of anything. Without this, a SELECT can walk out of the
        // namespace it was meant for - lakekeeper.system.snapshots, another team's table in the
        // same catalog - and the guard above would wave it through, because reading is all it does.
        if (!allowedNamespace.isEmpty()) {
            for (String table : tablesIn(plan)) {
                if (!table.startsWith(allowedNamespace + ".")) {
                    throw new RejectedException(
                            "Only tables in " + allowedNamespace + " can be queried here. "
                                    + "That statement reads " + table + ".");
                }
            }
        }
        // A query touching no table at all - SELECT 1, SELECT current_date() - is deliberately
        // allowed: there is no data for it to reach, so there is nothing to keep it out of.

        if (plan instanceof GlobalLimit) {
            return new CheckedSql(trimmed, false);
        }
        // Wrapping rather than appending, so it still works for set operations and ORDER BY.
        return new CheckedSql("SELECT * FROM (" + trimmed + ") LIMIT " + cap, true);
    }
}
