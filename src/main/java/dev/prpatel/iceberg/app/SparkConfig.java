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

    // When these are set, Spark signs its own S3 requests with the given key instead of asking the
    // catalog to sign each one. LakeKeeper turns on remote signing whenever STS is unavailable, and
    // the Hugging Face S3 gateway rejects those signed requests - it is happy with ordinary SigV4
    // from the client, which is what this restores. Empty by default, so MinIO is untouched.
    @Value("${app.s3.access-key:}")
    private String s3AccessKey;

    @Value("${app.s3.secret-key:}")
    private String s3SecretKey;

    @Value("${app.s3.endpoint:}")
    private String s3Endpoint;

    @Value("${app.s3.region:us-east-1}")
    private String s3Region;

    @Value("${app.s3.client-side-signing:false}")
    private boolean clientSideSigning;

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark;

        SparkSession.Builder builder = SparkSession.builder()
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
                // Hadoop's LocalFileSystem writes a hidden .<name>.crc checksum beside every file
                // it produces. On a Space the export directory is a mounted bucket, and the mount
                // refuses those names - "._SUCCESS.crc (Permission denied)" - which fails the whole
                // export. RawLocalFileSystem is the same filesystem without the checksum sidecars.
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

        if (clientSideSigning) {
            builder.config("spark.sql.catalog.lakekeeper.s3.remote-signing-enabled", "false")
                   .config("spark.sql.catalog.lakekeeper.s3.access-key-id", s3AccessKey)
                   .config("spark.sql.catalog.lakekeeper.s3.secret-access-key", s3SecretKey)
                   .config("spark.sql.catalog.lakekeeper.s3.endpoint", s3Endpoint)
                   .config("spark.sql.catalog.lakekeeper.s3.path-style-access", "true")
                   .config("spark.sql.catalog.lakekeeper.client.region", s3Region);
        }

        spark = builder.getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

        return spark;
    }
}

