package dev.prpatel.iceberg.app;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SparkConfig {

    // Defaults are the docker compose service names. When the whole stack runs inside a
    // single container (Hugging Face Spaces), the entrypoint overrides this with localhost.
    @Value("${app.catalog.uri:http://lakekeeper:8181/catalog}")
    private String catalogUri;

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark;

        spark = SparkSession.builder()
                .appName("Display Iceberg Table")
                .master("local[*]") // Use local mode for this example
                // Add the Iceberg SQL extensions for full functionality
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // Define a custom catalog name, e.g., 'lakekeeper_cat'
                .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
                // Specify that this catalog is an Iceberg RESTCatalog
                .config("spark.sql.catalog.lakekeeper.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
                // Provide the URI for your LakeKeeper REST endpoint
                .config("spark.sql.catalog.lakekeeper.uri", catalogUri)
                // Specify the warehouse name
                .config("spark.sql.catalog.lakekeeper.warehouse", "lakehouse")
                .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

        return spark;
    }
}

