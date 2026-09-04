package dev.prpatel.iceberg.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Writes the Iceberg table out as ordinary Parquet, ready to publish as a dataset.
 *
 * Deliberately a Spark write rather than a copy of the objects out of MinIO. Copying looks simpler
 * - the storage layer already holds Parquet - but it copies whatever is in the bucket, and Iceberg
 * keeps superseded files there until snapshots are expired. After a compaction, an overwrite or a
 * MERGE, a raw copy quietly republishes rows the table no longer contains. Reading through the
 * table means Spark resolves the current snapshot's manifest, so the export is what the table
 * actually is. It also lets us choose file sizes and give the files sensible names.
 *
 * Output goes to {@code data/export/<name>}, which is relative to the working directory - the same
 * convention used for downloaded CSVs - so on Spaces it lands on the mounted volume and is readable
 * afterwards as {@code hf://buckets/<owner>/<bucket>/export/<name>}.
 */
@Component
public class ParquetExporter {

    static final Path EXPORT_ROOT = Paths.get("data", "export");

    /** What an export produced, so the UI can say something more useful than "done". */
    public record Result(String name, String path, long rows, int files, long bytes) {
        public String humanBytes() {
            return bytes > 1024 * 1024 ? (bytes / (1024 * 1024)) + " MB" : (bytes / 1024) + " KB";
        }
    }

    private final SparkSession spark;

    public ParquetExporter(SparkSession spark) {
        this.spark = spark;
    }

    public Result export(String table, String name, int targetFiles) {
        String source = (table == null || table.isBlank())
                ? "lakekeeper.housing.staging_prices" : table.strip();
        String outName = (name == null || name.isBlank()) ? "uk-price-paid" : name.strip();
        int files = targetFiles <= 0 ? 8 : Math.min(targetFiles, 64);

        // Parquet goes in a data/ subdirectory with the card at the root. That is the layout the
        // Hub expects, and - the reason it matters here - it keeps README.md out of the directory
        // a reader points at. pyarrow and pandas fail on a folder that mixes parquet with anything
        // else, so a flat layout breaks pd.read_parquet("hf://buckets/.../export/<name>").
        Path target = EXPORT_ROOT.resolve(outName);
        Path dataDir = target.resolve("data");
        System.out.println("Exporting " + source + " -> " + dataDir.toAbsolutePath());

        Dataset<Row> df = spark.table(source);
        long rows = df.count();
        df.repartition(files)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(dataDir.toString());

        // Spark leaves _SUCCESS and crc sidecars behind; they are noise in a published dataset.
        tidy(dataDir);
        List<Path> written = parquetFiles(dataDir);
        long bytes = written.stream().mapToLong(ParquetExporter::sizeOf).sum();

        writeDatasetCard(target, source, outName, rows, written.size());
        System.out.println("✅ Exported " + rows + " rows to " + written.size() + " parquet files");
        return new Result(outName, target.toString(), rows, written.size(), bytes);
    }

    /** Parquet files currently sitting in an export, for the admin page. */
    public List<String> listExport(String name) {
        Path target = EXPORT_ROOT.resolve(name == null || name.isBlank() ? "uk-price-paid" : name)
                .resolve("data");
        return parquetFiles(target).stream().map(p -> p.getFileName().toString()).sorted().toList();
    }

    private static List<Path> parquetFiles(Path dir) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".parquet"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    private static long sizeOf(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return 0L;
        }
    }

    private static void tidy(Path dir) {
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> junk = new ArrayList<>(s.filter(p -> {
                String n = p.getFileName().toString();
                return n.equals("_SUCCESS") || n.startsWith(".") || n.endsWith(".crc");
            }).toList());
            for (Path p : junk) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            System.err.println("Could not tidy the export directory: " + e.getMessage());
        }
    }

    /**
     * A dataset card, so the published repo arrives explaining itself rather than as a bare folder
     * of parquet. The YAML front matter is what the Hub reads for the dataset page.
     */
    private static void writeDatasetCard(Path dir, String source, String name, long rows, int files) {
        String card = """
                ---
                license: other
                license_name: open-government-licence-3.0
                license_link: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
                language:
                  - en
                size_categories:
                  - 1M<n<10M
                task_categories:
                  - tabular-regression
                tags:
                  - uk
                  - property
                  - land-registry
                configs:
                  - config_name: default
                    data_files: "data/*.parquet"
                ---

                # %s

                HM Land Registry Price Paid data, exported from an Apache Iceberg table
                (`%s`) as plain Parquet.

                - **Rows:** %,d
                - **Files:** %d

                Each row is a residential property sale in England or Wales. Coded columns are worth
                reading before filtering on them:

                | Column | Meaning |
                |--------|---------|
                | `property_type` | `D` detached, `S` semi-detached, `T` terraced, `F` flat/maisonette, `O` other |
                | `duration` | `F` freehold, `L` leasehold |
                | `ppd_category_type` | `A` standard sale, `B` includes repossessions, buy-to-let and bulk transfers |
                | `record_status` | `A` addition, `C` change, `D` delete |

                > Category `B` rows include portfolio sales recorded against a single address, so they
                > can be very large. Exclude them before computing averages.

                Source: HM Land Registry Price Paid Data, published under the Open Government Licence v3.0.
                Contains HM Land Registry data © Crown copyright and database right.
                """.formatted(name, source, rows, files);
        try {
            Files.writeString(dir.resolve("README.md"), card, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the dataset card", e);
        }
    }
}
