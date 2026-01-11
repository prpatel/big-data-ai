package dev.prpatel.iceberg.app;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import static dev.prpatel.iceberg.app.Utilities.formatDataSet;
import static dev.prpatel.iceberg.app.Utilities.formatForHtml;

@RestController
public class IcebergController {

    @Autowired
    private SparkSession spark;

    @GetMapping("/getCatalog")
    public String getCatalog() {
        System.out.printf("Connecting to Lakekeeper running on localhost:8181\n");

        StringBuilder sb = new StringBuilder("<pre>");
        try (RESTCatalog catalog = new RESTCatalog()) {

            // 2. Set the configuration properties for the catalog
            Map<String, String> properties = new HashMap<>();
            // Lakekeeper URL running in docker
            properties.put("uri", "http://localhost:8181/catalog");
            // This is the name of the warehouse name created in Lakekeeper
            properties.put("warehouse", "lakehouse");
            // Add any necessary credential properties here, e.g.:
            // properties.put("token", "your-bearer-token");

            catalog.initialize("lakekeeper", properties);

            // 3. Define the identifier for the table you want to access
            String namespace = "housing";
            String tableName = "staging_prices";
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

            sb.append("Attempting to load table: " + tableIdentifier);

            // 4. Load the table metadata from the catalog
            Table table = catalog.loadTable(tableIdentifier);

            // 5. If successful, print some of the table's metadata
            sb.append("Successfully loaded table!");
            sb.append("\nTable Location: " + table.location());
            sb.append("\nlocationProvider: " + table.locationProvider());
            sb.append("\nTable Location: " + table.toString());
            sb.append("\nCurrent Snapshot ID: " + table.currentSnapshot().snapshotId());
            sb.append("\nSchema: " + table.schema());

            System.out.println(sb);

        } catch (NoSuchTableException e) {
            System.err.println("Error: The table or namespace does not exist.");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An error occurred while communicating with the REST catalog.");
            System.err.println("Please ensure the endpoint is a compliant Iceberg REST Catalog.");
            e.printStackTrace();
        }
        sb.append("</pre>");
        return sb.toString();
    }

    @GetMapping("/showtable")
    public String showtable() {
        String tableName = "lakekeeper.housing.staging_prices";
        System.out.println("Spark Config:" + spark.conf().getAll());

        try {
            System.out.println("Reading table: " + tableName);
            Dataset<Row> tableDF = spark.read().table(tableName);
            System.out.println("Displaying the first 10 rows:");
            tableDF.show(10);
            return String.format("<pre>First 10 rows of table\n %s</pre>", formatForHtml(formatDataSet(tableDF, 10)));
        } catch (Exception e) {
            System.err.println("An error occurred while reading the Iceberg table.");
            e.printStackTrace();
        }
        return "Error loading table";
    }

    @GetMapping("/runquery")
    public String runquery() {
        spark.sparkContext().setLogLevel("WARN");
        String output="no result";

        try {

            String query;
            query= """
SELECT *
FROM lakekeeper.housing.staging_prices
WHERE town = 'BIRMINGHAM'
AND price BETWEEN 300000 AND 400000;""";


            query = """
			   SELECT transaction_id, price, date_of_transfer, postcode, property_type, new_property, duration, paon, 
			          saon, street, 
			          locality, town, district, county, ppd_category_type, record_status FROM lakekeeper.housing.staging_prices 
			          WHERE LOWER(town) LIKE '%london%' OR LOWER(county) LIKE '%london%' OR LOWER(district) LIKE '%london%' 
			   		  OR LOWER(postcode) LIKE '%london%'""";

            System.out.println("running query: " + query);
            Dataset<Row> resultsDF = spark.sql(query);

            System.out.println("--- Query Results ---");
            long count = resultsDF.count();
            System.out.println("Count:" + count);
            resultsDF.show();

            output = formatForHtml(formatDataSet(resultsDF, 10));
        } catch (Exception e) {
            System.err.println("An error occurred while reading the Iceberg table.");
            e.printStackTrace();
        } finally {

        }

        return String.format("Result:\n %s!", output);
    }
}
