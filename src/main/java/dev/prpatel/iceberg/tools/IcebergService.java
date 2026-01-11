package dev.prpatel.iceberg.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class IcebergService {

    private final SparkSession spark;

    @Autowired
    public IcebergService(SparkSession spark) {
        this.spark = spark;
    }

    public void download(String year) {

        Path outputDir = Paths.get("./data/house_prices");

        String baseUrl = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com";
        List<String> urls = new ArrayList<>();
        int startYear = 2015;
        int endYear = 2025;

        if (year != null && !year.isEmpty()) {
            urls.add(baseUrl + "/pp-" + year + ".csv");
            downloadFiles(urls, outputDir);
        } else {

            for (int y = startYear; y <= endYear; y++) {
                urls.add(baseUrl + "/pp-" + y + ".csv");
            }
            downloadFiles(urls, outputDir);
        }
        System.out.println("All files downloaded successfully to " + outputDir);
    }

    public void load(String year) {
        // Create Namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakekeeper.housing");

        // Define Schema
        // Note: Spark SQL DDL is often easier than constructing StructTypes manually for CREATE TABLE
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS lakekeeper.housing.staging_prices (
                transaction_id STRING COMMENT 'A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.',
                price INT COMMENT 'Sale price stated on the transfer deed.',
                date_of_transfer DATE COMMENT 'Date when the sale was completed, as stated on the transfer deed.',
                postcode STRING COMMENT 'This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.',
                property_type STRING COMMENT 'D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other',
                new_property STRING COMMENT 'Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = a newly built property, N = an established residential building',
                duration STRING COMMENT 'Relates to the tenure: F = Freehold, L= Leasehold etc. Note that HM Land Registry does not record leases of 7 years or less in the Price Paid Dataset.',
                paon STRING COMMENT 'Primary Addressable Object Name. Typically the house number or name',
                saon STRING COMMENT 'Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.',
                street STRING,
                locality STRING,
                town STRING,
                district STRING,
                county STRING,
                ppd_category_type STRING COMMENT 'Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.',
                record_status STRING COMMENT 'Indicates additions, changes and deletions to the records. A = Addition C = Change D = Delete'
            )
            USING iceberg
            LOCATION 's3://warehouse/housing/staging'
        """;

        spark.sql(createTableSql);

        // Define CSV Schema for reading
        StructType csvSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("transaction_id", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.IntegerType, true),
                DataTypes.createStructField("date_of_transfer_str", DataTypes.StringType, true), // Read as string first to parse date
                DataTypes.createStructField("postcode", DataTypes.StringType, true),
                DataTypes.createStructField("property_type", DataTypes.StringType, true),
                DataTypes.createStructField("new_property", DataTypes.StringType, true),
                DataTypes.createStructField("duration", DataTypes.StringType, true),
                DataTypes.createStructField("paon", DataTypes.StringType, true),
                DataTypes.createStructField("saon", DataTypes.StringType, true),
                DataTypes.createStructField("street", DataTypes.StringType, true),
                DataTypes.createStructField("locality", DataTypes.StringType, true),
                DataTypes.createStructField("town", DataTypes.StringType, true),
                DataTypes.createStructField("district", DataTypes.StringType, true),
                DataTypes.createStructField("county", DataTypes.StringType, true),
                DataTypes.createStructField("ppd_category_type", DataTypes.StringType, true),
                DataTypes.createStructField("record_status", DataTypes.StringType, true)
        });

        List<String> filesToLoad = new ArrayList<>();
        if (year != null && !year.isEmpty()) {
            filesToLoad.add("data/house_prices/pp-" + year + ".csv");
        } else {
            File folder = new File("data/house_prices");
            if (folder.exists() && folder.isDirectory()) {
                File[] files = folder.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile() && !file.getName().equals(".gitkeep")) {
                            filesToLoad.add(file.getPath());
                        }
                    }
                }
            }
        }

        for (String csvPath : filesToLoad) {
            System.out.println("Reading CSV from " + csvPath);
            File csvFile = new File(csvPath);
            if (!csvFile.exists()) {
                System.out.println("File not found: " + csvPath);
                continue;
            }

            Dataset<Row> df = spark.read()
                    .option("header", "false")
                    .schema(csvSchema)
                    .csv(csvPath);

            // Transform: Parse date string to DateType
            Dataset<Row> transformedDf = df.withColumn("date_of_transfer",
                            org.apache.spark.sql.functions.to_date(df.col("date_of_transfer_str"), "yyyy-MM-dd HH:mm"))
                    .drop("date_of_transfer_str")
                    // Reorder columns to match table schema if necessary, though by name usually works
                    .select("transaction_id", "price", "date_of_transfer", "postcode", "property_type",
                            "new_property", "duration", "paon", "saon", "street", "locality", "town",
                            "district", "county", "ppd_category_type", "record_status");

            // Write to Iceberg Table
            System.out.println("Writing to Iceberg table...");
            try {
                transformedDf.writeTo("lakekeeper.housing.staging_prices")
                        .append();
                System.out.println("✅ Data loaded successfully from " + csvPath);
            } catch (Exception e) {
                System.err.println("Error writing to table from " + csvPath + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void clear() {
        System.out.println("Deleting data...");
        
        // Use Spark SQL to drop the table if it exists
        // This is more idiomatic when using Spark with Iceberg
        try {
            spark.sql("DROP TABLE IF EXISTS lakekeeper.housing.staging_prices PURGE");
            System.out.println("✅ Table lakekeeper.housing.staging_prices dropped successfully!");
        } catch (Exception e) {
             System.err.println("Error dropping table: " + e.getMessage());
        }
        
        System.out.println("✅ Data cleared successfully!");
    }

    public void setup() {
        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:9000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio1234")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();

        System.out.println("Creating bucket...");
        createBucket(s3, "warehouse");
        System.out.println("✅ Bucket created successfully");

        System.out.println("Bootstrapping project...");
        HttpClient client = HttpClient.newHttpClient();
        String baseUrl = "http://localhost:8181";

        try {
            bootstrapProject(client, baseUrl);
            System.out.println("✅ Project bootstrapped successfully");
        } catch (Exception e) {
            if (e.getMessage().contains("400")) {
                System.out.println("Catalog already bootstrapped - skipping...");
            } else {
                System.err.println("Something went wrong: " + e.getMessage());
            }
        }

        System.out.println("Creating warehouse...");
        try {
            createWarehouse(client, baseUrl, "warehouse");
            System.out.println("✅ Warehouse created successfully");
        } catch (Exception e) {
            if (e.getMessage().contains("400")) {
                System.out.println("Warehouse already exists - skipping...");
            } else {
                System.err.println("Something went wrong: " + e.getMessage());
            }
        }
    }

    /**
     * Verifies that the Iceberg catalog is correctly configured and accessible.
     */
    public void checkSetup() {
        try {
            System.out.println("Verifying Iceberg Catalog setup...");
            spark.sql("USE lakekeeper");
            Dataset<Row> namespaces = spark.sql("SHOW NAMESPACES");
            System.out.println("Current namespaces:");
            namespaces.show(false);
            System.out.println("✅ Setup verification passed: Catalog is accessible.");
        } catch (Exception e) {
            System.err.println("❌ Setup verification failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public List<String> listDownloadedFiles() {
        List<String> fileNames = new ArrayList<>();
        File folder = new File("data/house_prices");
        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && !file.getName().equals(".gitkeep")) {
                        fileNames.add(file.getName());
                    }
                }
            }
        }
        return fileNames;
    }

    public static void downloadFiles(List<String> urls, Path outputDir) {
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directory", e);
        }

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(urls.size(), 10));

        List<CompletableFuture<Void>> futures = urls.stream()
                .map(url -> CompletableFuture.runAsync(() -> downloadFile(client, url, outputDir), executor))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private static void downloadFile(HttpClient client, String url, Path outputDir) {
        String filename = url.substring(url.lastIndexOf('/') + 1);
        Path outputFile = outputDir.resolve(filename);

        if (Files.exists(outputFile)) {
            System.out.println("Skipped: " + filename + " (already exists)");
            return;
        }

        System.out.println("Downloading " + filename + "...");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() == 200) {
                Files.copy(response.body(), outputFile, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("✓ Downloaded: " + filename);
            } else {
                System.err.println("Failed: " + filename + " (Status: " + response.statusCode() + ")");
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error downloading " + url + ": " + e.getMessage());
        }
    }

    // Note: This interacts with the LakeKeeper Management API, not the Iceberg REST Catalog Protocol.
    // Therefore, we use HttpClient instead of RESTCatalog.
    public static void bootstrapProject(HttpClient client, String baseUrl) throws Exception {
        String payload = "{\"accept-terms-of-use\": true}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/management/v1/bootstrap"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new RuntimeException("Failed to bootstrap project: " + response.statusCode() + " " + response.body());
        }
    }

    public static void createBucket(S3Client s3, String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        } catch (S3Exception e) {
            if (e.statusCode() != 409) { // 409 Conflict means bucket exists
                throw e;
            }
        }
    }

    // Note: This interacts with the LakeKeeper Management API, not the Iceberg REST Catalog Protocol.
    // Therefore, we use HttpClient instead of RESTCatalog.
    public static void createWarehouse(HttpClient client, String baseUrl, String storageBucket) throws Exception {
        String payload = """
            {
                "warehouse-name": "lakehouse",
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                    "type": "s3",
                    "bucket": "%s",
                    "assume-role-arn": null,
                    "endpoint": "http://minio:9000",
                    "region": "us-east-1",
                    "path-style-access": true,
                    "flavor": "minio",
                    "sts-enabled": true
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "aws-access-key-id": "minio",
                    "aws-secret-access-key": "minio1234"
                }
            }
            """.formatted(storageBucket);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/management/v1/warehouse"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new RuntimeException("Failed to create warehouse: " + response.statusCode() + " " + response.body());
        }
    }

}
