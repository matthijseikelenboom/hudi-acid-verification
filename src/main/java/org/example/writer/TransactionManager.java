package org.example.writer;

import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.example.reader.ReaderThread;
import org.example.transactionlog.Transaction;
import org.example.transactionlog.TransactionLog;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionManager {
    private final Configuration configuration;
    private final SparkSession session;
    private final TransactionLog transactionLog;
    private final TransactionGenerator transactionGenerator;
    private final String fullyQualifiedTableName;
    private final AtomicInteger transactionCount;
    private final AtomicBoolean stopReadersAndWriters;
    private final AtomicInteger failedVerificationCount;

    @Getter
    private boolean hasFailedWriters;

    @Getter
    private boolean hasFailedReaders;

    public TransactionManager(Configuration configuration, SparkSession session) {
        this.configuration = configuration;
        this.session = session;
        this.transactionLog = new TransactionLog();
        this.transactionGenerator = new TransactionGenerator(configuration);
        this.fullyQualifiedTableName = String.format("%s.%s", configuration.getDatabaseName(), configuration.getTableName());
        this.transactionCount = new AtomicInteger();
        this.stopReadersAndWriters = new AtomicBoolean(false);
        this.failedVerificationCount = new AtomicInteger();
    }

    public void run() throws InterruptedException {
        createDatabaseIfNotExists();
        createOrRecreateTable();

        int numberOfSparkSessionsForWriters = configuration.getNumberOfSparkSessionsForWriters();
        int numberOfSparkSessionsForReaders = configuration.getNumberOfSparkSessionsForReaders();
        SparkSession[] sparkSessionForWriters = createSparkSessions(numberOfSparkSessionsForWriters);
        SparkSession[] sparkSessionForReaders = createSparkSessions(numberOfSparkSessionsForReaders);

        hasFailedReaders = false;
        int numberOfReaderThreads = configuration.getNumberOfReaderThreads();
        ReaderThread[] readerThreads = createAndStartReaderThreads(numberOfReaderThreads, sparkSessionForReaders, numberOfSparkSessionsForReaders);

        hasFailedWriters = false;
        int numberOfWriterThreads = configuration.getNumberOfWriterThreads();
        TransactionWriter[] writerThreads = createAndStartTransactionWriters(numberOfWriterThreads, sparkSessionForWriters, numberOfSparkSessionsForWriters);

        for (TransactionWriter writerThread : writerThreads) {
            writerThread.join();
            hasFailedWriters = hasFailedWriters && writerThread.getWriterException() != null;
        }

        stopReadersAndWriters.set(true);

        for (ReaderThread readerThread : readerThreads) {
            readerThread.join();
            hasFailedReaders = hasFailedReaders && readerThread.getReaderException() != null;
        }
        System.out.println("ACID Verification finished!");
    }

    private void createDatabaseIfNotExists() {
        session.sql("CREATE SCHEMA IF NOT EXISTS " + configuration.getDatabaseName());
    }

    private void createOrRecreateTable() {
        session.sql("DROP TABLE IF EXISTS " + fullyQualifiedTableName);
        session.sql(String.format("CREATE TABLE IF NOT EXISTS %s(\n" +
                "primaryKeyValue STRING,\n" +
                "partitionKeyValue STRING,\n" +
                "dataValue STRING)\n" +
                "USING hudi\n" +
                "PARTITIONED BY (partitionKeyValue)\n" +
                "TBLPROPERTIES (\n" +
                "primaryKey = 'primaryKeyValue',\n" +
                "preCombinedField = 'dataValue')", fullyQualifiedTableName));
    }

    public boolean hasFailedVerification() {
        return failedVerificationCount.get() > 0;
    }

    private SparkSession[] createSparkSessions(final int numberOfSparkSessions) {
        SparkSession[] childSessions = new SparkSession[numberOfSparkSessions];
        for (int sessionNumber = 0; sessionNumber < numberOfSparkSessions; sessionNumber++) {
            childSessions[sessionNumber] = session.cloneSession();
        }
        return childSessions;
    }

    private ReaderThread[] createAndStartReaderThreads(final int numberOfReaderThreads, final SparkSession[] sessions, final int numberOfSparkSessions) {
        ReaderThread[] readerThreads = new ReaderThread[numberOfReaderThreads];
        for (int readerNumber = 0; readerNumber < numberOfReaderThreads; readerNumber++) {
            SparkSession childSession = sessions[readerNumber % numberOfSparkSessions];
            readerThreads[readerNumber] = new ReaderThread(transactionLog, childSession, fullyQualifiedTableName, stopReadersAndWriters, this::failedVerificationCallback);
            readerThreads[readerNumber].setName("acid-reader-" + readerNumber);
            readerThreads[readerNumber].start();
        }
        return readerThreads;
    }

    private TransactionWriter[] createAndStartTransactionWriters(final int numberOfWriterThreads, final SparkSession[] sessions, final int numberOfSparkSessions) {
        TransactionWriter[] writerThreads = new TransactionWriter[numberOfWriterThreads];
        for (int writerNumber = 0; writerNumber < numberOfWriterThreads; writerNumber++) {
            SparkSession childSession = sessions[writerNumber % numberOfSparkSessions];
            writerThreads[writerNumber] = new TransactionWriter(
                    transactionLog,
                    this::provideTransactionIfLimitNotReached,
                    transactionGenerator::transactionCommitted,
                    childSession,
                    configuration.getDatabaseName(),
                    configuration.getTableName(),
                    stopReadersAndWriters
            );
            writerThreads[writerNumber].setName("acid-writer-" + writerNumber);
            writerThreads[writerNumber].start();
        }
        return writerThreads;
    }

    private Transaction provideTransactionIfLimitNotReached() {
        final int transactionNumber = transactionCount.incrementAndGet();
        if (transactionNumber <= configuration.getTotalNumberOfTransactions()) {
            System.out.println("Starting next transaction: " + transactionNumber);
            return transactionGenerator.getNextTransaction();
        } else {
            stopReadersAndWriters.set(true);
            return null;
        }
    }

    private void failedVerificationCallback() {
        this.failedVerificationCount.incrementAndGet();
        this.stopReadersAndWriters.set(true);
    }
}
