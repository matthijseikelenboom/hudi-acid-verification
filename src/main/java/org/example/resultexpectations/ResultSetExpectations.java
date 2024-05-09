package org.example.resultexpectations;

import org.example.resultset.Record;
import org.example.resultset.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ResultSetExpectations {
    private final Map<String, Expectation> expectationPerPrimaryKeyValue = new HashMap<>();

    public void setRecordExpectation(String primaryKeyValue, Expectation expectation) {
        expectationPerPrimaryKeyValue.put(primaryKeyValue, expectation);
    }

    public Optional<Expectation> getRecordExpectation(String primaryKeyValue) {
        return Optional.ofNullable(expectationPerPrimaryKeyValue.get(primaryKeyValue));
    }

    public boolean isStatisfied(ResultSet resultSet) {
        boolean satisfied = true;

        for (Expectation expectation : expectationPerPrimaryKeyValue.values()) {
            boolean expectationStatisfied = expectation.isSatisfied(resultSet);
            if (!expectationStatisfied) {
                System.err.println("Expectation not satisfied: " + expectation);
                satisfied = false;
            }
        }

        // Check no other primary key value
        Set<String> primaryKeyValuesWithExpectations = expectationPerPrimaryKeyValue.keySet();
        for (Record r : resultSet.getRecords()) {
            if (!primaryKeyValuesWithExpectations.contains(r.getPrimaryKeyValue())) {
                System.err.println("Unexpected primary key in the result set: " + r.getPrimaryKeyValue());
                satisfied = false;
            }
        }

        return satisfied;
    }

}
