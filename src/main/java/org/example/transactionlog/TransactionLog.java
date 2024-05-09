package org.example.transactionlog;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransactionLog {
    private final List<TransactionLogEvent> transactionLogEvents = new LinkedList<>();

    public synchronized void add(TransactionLogEvent event) {
        Objects.requireNonNull(event);
        transactionLogEvents.add(event);
    }

    public void logIntent(Transaction transaction) {
        TransactionLogEvent event = new TransactionLogEvent(EventType.TRANSACTION_INTENDED, transaction);
        add(event);
    }

    public void logCommit(Transaction transaction) {
        TransactionLogEvent event = new TransactionLogEvent(EventType.TRANSACTION_COMMITTED, transaction);
        add(event);
    }

    public synchronized List<TransactionLogEvent> getFirstNEvents(int n) {
        return transactionLogEvents
                .stream()
                .limit(n)
                .collect(Collectors.toList());
    }

    public synchronized int getEventCount() {
        return transactionLogEvents.size();
    }

}
