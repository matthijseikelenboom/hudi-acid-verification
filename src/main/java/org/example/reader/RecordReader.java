package org.example.reader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.example.resultexpectations.ResultSetExpectations;
import org.example.resultset.Record;
import org.example.resultset.ResultSet;

import java.util.List;

@Getter
@AllArgsConstructor(staticName = "create")
public class RecordReader {
    private final ResultSetExpectations resultSetExpectations;
    private final SparkSession session;

    public boolean readAndVerifyData() {
        ResultSet result = readData();

        return verifyResultSet(result);
    }

    public ResultSet readData() {
        List<Record> recordDataSet = session
                .sql("SELECT * FROM record;")
                .as(Record.getEncoder())
                .collectAsList();

        return new ResultSet(recordDataSet);
    }

    public boolean verifyResultSet(ResultSet resultSet) {
        return resultSetExpectations.isStatisfied(resultSet);
    }

}
