package org.example.writer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.resultset.InconsistentResultSetException;
import org.example.resultset.Record;
import org.example.transactionlog.*;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class TransactionWriter extends Thread {
    private static final Set<String> KNOWN_TRANSACTION_FAILURE_CLASSES = Set.of(
            "org.apache.hudi.exception.HoodieException",
            "org.apache.hudi.exception.HoodieCompactionException",
            "org.apache.hudi.exception.HoodieRollbackException",
            "org.apache.hudi.exception.HoodieWriteConflictException",
            "org.apache.spark.SparkException"
    );

    private final TransactionLog transactionLog;
    private final Supplier<Transaction> transactionSupplier;
    private final Consumer<Transaction> transactionCommittedConsumer;
    private final SparkSession session;
    private final String databaseName;
    private final String tableName;
    private final AtomicInteger tempViewNumber;
    private final AtomicBoolean stopWriter;

    @Getter
    private Exception writerException;


    public TransactionWriter(
            TransactionLog transactionLog,
            Supplier<Transaction> transactionSupplier,
            Consumer<Transaction> transactionCommittedConsumer,
            SparkSession session,
            String databaseName,
            String tableName,
            AtomicBoolean stopWriter
    ) {
        this.transactionLog = transactionLog;
        this.transactionSupplier = transactionSupplier;
        this.transactionCommittedConsumer = transactionCommittedConsumer;
        this.session = session;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tempViewNumber = new AtomicInteger();
        this.stopWriter = stopWriter;
    }

    @Override
    public void run() {
        log.info("{} started.", Thread.currentThread().getName());
        try {
            while (!stopWriter.get()) {
                var transaction = transactionSupplier.get();
                if (transaction != null) {
                    log.info("[Writer] Handling transaction {}", transaction.transactionId);
                    handleTransaction(transaction);
                }
            }
        } catch (Exception e) {
            log.error("Exception in writer.", e);
            writerException = e;
        }
        log.info("{} finished.", Thread.currentThread().getName());
    }

    private void handleTransaction(final Transaction transaction) {
        transactionLog.add(new TransactionLogEvent(EventType.TRANSACTION_INTENDED, transaction));
        var timeBeforeTransaction = System.currentTimeMillis();
        withRetryOnException(() -> {
            switch (transaction.manipulationType) {
                case INSERT:
                    insertTransaction(transaction);
                    break;
                case UPDATE:
                    updateTransaction(transaction);
                    break;
                case DELETE:
                    deleteTransaction(transaction);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown manipulationType: " + transaction.manipulationType);
            }
        });
        var transactionDuration = System.currentTimeMillis() - timeBeforeTransaction;
        log.info("Acid Verification threadType='writer' manipulationType={} duration={}", transaction.manipulationType, transactionDuration);
        transactionCommittedConsumer.accept(transaction);
        transactionLog.logCommit(transaction);
    }

    private void withRetryOnException(DataManipulationTransaction transaction) {
        var retryCount = 0;
        var ranSuccessfully = false;
        while (!ranSuccessfully) {
            try {
                transaction.run();
                ranSuccessfully = true;
            } catch (SparkException e) {
                log.error("Transaction failed.", e);
                if (retryCount >= 100) {
                    throw new RuntimeException(e);
                }
                retryCount++;
            }
        }
    }

    private void insertTransaction(Transaction transaction) {
        tryTransaction(() -> {
            final var records = transaction.dataManipulations
                    .stream()
                    .map(TransactionWriter::mapToRecord)
                    .collect(Collectors.toList());

            var warehouseDir = session.conf().get("spark.sql.warehouse.dir");
            var tablePath = warehouseDir + "/" + databaseName + ".db/" + tableName;
            var dataSet = session.createDataset(records, Record.getEncoder());

            dataSet.write().format("hudi")
                    .option("hoodie.table.name", tableName)
                    .option("hoodie.datasource.write.recordkey.field", "primaryKeyValue")
                    .option("hoodie.datasource.write.partitionpath.field", "partitionKeyValue")
                    .option("hoodie.datasource.write.precombine.field", "dataValue")
                    .mode(SaveMode.Append)
                    .save(tablePath);
        });
    }

    private void updateTransaction(Transaction transaction) throws InconsistentResultSetException {
        tryTransaction(() -> {
            final var records = transaction.dataManipulations
                    .stream()
                    .map(TransactionWriter::mapToRecord)
                    .collect(Collectors.toList());

            try {
                var rowsToUpdate = session.createDataset(records, Record.getEncoder());
                var tempViewName = "temp_view_" + tempViewNumber.incrementAndGet();
                rowsToUpdate.createTempView(tempViewName);
                var updateStatement = "MERGE INTO " + databaseName + "." + tableName + " t \n" +
                        "USING (SELECT * FROM " + tempViewName +") s \n" +
                        "ON t.primaryKeyValue = s.primaryKeyValue \n" +
                        "WHEN MATCHED THEN UPDATE SET t.dataValue = s.dataValue " +
                        "WHEN NOT MATCHED THEN " +
                        "INSERT (t.primaryKeyValue, t.partitionKeyValue, t.dataValue) VALUES (s.primaryKeyValue, s.partitionKeyValue, s.dataValue);";
                System.out.println(updateStatement);
                session.sql(updateStatement);
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void deleteTransaction(Transaction transaction) throws InconsistentResultSetException {
        tryTransaction(() -> {
            var primaryKeyValues = transaction.dataManipulations
                    .stream()
                    .map(dataManipulation -> "\"" + dataManipulation.primaryKeyValue + "\"")
                    .collect(Collectors.joining());

            var deleteStatement = String.format("DELETE FROM %s WHERE primaryKeyValue IN (%s)", databaseName + "." + tableName, primaryKeyValues);
            session.sql(deleteStatement);
        });
    }

    private void tryTransaction(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            System.out.println("ERROR! : " + t.getMessage());
            wrapOrRethrowException(t);
        }
    }

    private void wrapOrRethrowException(Throwable t) {
        if (KNOWN_TRANSACTION_FAILURE_CLASSES.contains(t.getClass().getName())) {
            throw new TransactionFailedException(t);
        } else {
            throw new RuntimeException(t);
        }
    }

    private static Record mapToRecord(DataManipulation dataManipulation) {
        return new Record(dataManipulation.primaryKeyValue, dataManipulation.partitionKeyValue, dataManipulation.dataValue);
    }

    private interface DataManipulationTransaction {
        void run() throws SparkException;
    }
}
