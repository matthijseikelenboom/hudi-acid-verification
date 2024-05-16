package org.example.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.example.resultexpectations.ResultSetExpectationProducer;
import org.example.resultexpectations.ResultSetExpectations;
import org.example.resultset.Record;
import org.example.resultset.ResultSet;
import org.example.transactionlog.TransactionLog;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ReaderThread extends Thread {
    private final TransactionLog transactionLog;
    private final SparkSession session;
    private final String fullyQualifiedTableName;
    private final AtomicBoolean stopReader;
    private final ResultSetExpectationProducer resultSetExpectationProducer;
    private final Runnable verificationFailedCallback;

    @Getter
    private Exception readerException;

    public ReaderThread(
            TransactionLog transactionLog,
            SparkSession session,
            String fullyQualifiedTableName,
            AtomicBoolean stopReader,
            Runnable verificationFailedCallback
    ) {
        this.transactionLog = transactionLog;
        this.session = session;
        this.fullyQualifiedTableName = fullyQualifiedTableName;
        this.stopReader = stopReader;
        this.resultSetExpectationProducer = new ResultSetExpectationProducer(transactionLog);
        this.verificationFailedCallback = verificationFailedCallback;
    }

    @Override
    public void run() {
        try {
            while (!stopReader.get()) {
                performVerification();
            }
        } catch (Exception e) {
            log.error("Exception in reader.", e);
            readerException = e;
        }
    }

    private void performVerification() {
        final int eventCountBeforeRead = transactionLog.getEventCount();
        final long timeBeforeRead = System.currentTimeMillis();
        final ResultSet resultSet = readData();
        final long readDuration = System.currentTimeMillis() - timeBeforeRead;
        final int eventCountAfterRead = transactionLog.getEventCount();
        final ResultSetExpectations resultSetExpectations = resultSetExpectationProducer.createResultSetExpectations(eventCountBeforeRead, eventCountAfterRead);
        final boolean satisfied = resultSetExpectations.isStatisfied(resultSet);
        if (!satisfied) {
            log.error("Verification Failed. ResultSet:\n{}", resultSet);
            verificationFailedCallback.run();
        }
        log.info(
                "Acid Verification threadType='reader' satisfied='{}' duration={} eventCountBeforeRead={} eventCountAfterRead={} resultSetSize={}",
                satisfied,
                readDuration,
                eventCountBeforeRead,
                eventCountAfterRead,
                resultSet.getRecords().size()
        );
    }

    public ResultSet readData() {
        session.sql("REFRESH TABLE " + fullyQualifiedTableName);
        session.sql("MSCK REPAIR TABLE" + fullyQualifiedTableName);
        List<Record> recordDataSet = session
                .sql("SELECT * FROM " + fullyQualifiedTableName)
                .as(Record.getEncoder())
                .collectAsList();

        return new ResultSet(recordDataSet);
    }
}
