package org.example;

import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;

public class SparkSessionProvider {
    private final Path dataDir;
    private final int DRIVER_PORT = 40949;
    private final int BLOCK_MANAGER_PORT = DRIVER_PORT + 1;
    private final Network sparkNetwork;
    private GenericContainer<?> metastoreContainer;

    /**
     * @param dataDir           The location where the lakehouse will store the data
     * @param startContainers   Whether this class should start a Hive metastore or not
     */
    public SparkSessionProvider(Path dataDir, boolean startContainers) {
        this.dataDir = dataDir;
        this.sparkNetwork = Network.newNetwork();

        if (startContainers) {
            this.metastoreContainer = createHiveMetastoreContainer().withStartupTimeout(Duration.ofSeconds(120));

            Startables.deepStart(metastoreContainer).join();
        }
    }

    private GenericContainer<?> createHiveMetastoreContainer() {
        return new GenericContainer<>(DockerImageName.parse("apache/hive:4.0.0-beta-1"))
                .withEnv("SERVICE_NAME", "metastore")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.HiveMetastore")))
                .withNetwork(sparkNetwork)
                .withExposedPorts(9083)
                .withNetworkAliases("hivemetastore");
    }

    public SparkSession getSession() {
        var sparkMasterAddress = "local[*]";
        var hivePort = 9083;
        if (metastoreContainer != null) {
            hivePort = metastoreContainer.getFirstMappedPort();
        }
        return SparkSession
                .builder()
                .master(sparkMasterAddress)

                .config("spark.driver.host", "localhost")
                .config("spark.driver.port", DRIVER_PORT)
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.blockManager.port", BLOCK_MANAGER_PORT)

                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.warehouse.dir", dataDir.toString())

                .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")

                .config("hive.metastore.uris", "thrift://localhost:" + hivePort)

                .enableHiveSupport()
                .getOrCreate();
    }

}
