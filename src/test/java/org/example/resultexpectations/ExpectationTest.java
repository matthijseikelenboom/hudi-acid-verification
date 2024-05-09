package org.example.resultexpectations;

import org.example.resultset.Record;
import org.example.resultset.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.example.TestDataFactory.createRecord;
import static org.example.TestDataFactory.createUpdatedRecord;

class ExpectationTest {
    private static final Record RECORD_1_INITIAL = createRecord(1);
    private static final Record RECORD_1_UPDATED = createUpdatedRecord(RECORD_1_INITIAL);
    private static final Record RECORD_2_INITIAL = createRecord(2);
    private static final Record RECORD_2_UPDATED = createUpdatedRecord(RECORD_2_INITIAL);
    private static final Record RECORD_3_INITIAL = createRecord(3);

    @ParameterizedTest
    @MethodSource
    void expectRecordPresence(Record record, ResultSet resultSet, boolean expectedResult) {
        // Given
        final ExpectRecordPresence expectation = ExpectRecordPresence.create(record);

        // When
        final boolean satisfied = expectation.isSatisfied(resultSet);

        // Then
        assertThat(satisfied).isEqualTo(expectedResult);
    }

    @Test
    void expectRecordPresence_withDuplicateRow() {
        // Given
        final ResultSet resultSet = new ResultSet(Arrays.asList(RECORD_1_INITIAL, RECORD_1_UPDATED));
        final ExpectRecordPresence expectation = ExpectRecordPresence.create(RECORD_1_INITIAL);

        // When + Then
        assertThatThrownBy(() -> expectation.isSatisfied(resultSet)).isInstanceOf(RuntimeException.class);
    }

    @ParameterizedTest
    @MethodSource
    void expectRecordAbsence(Record record, ResultSet resultSet, boolean expectedResult) {
        // Given
        final ExpectRecordAbsence expectation = ExpectRecordAbsence.create(record);

        // When
        final boolean satisfied = expectation.isSatisfied(resultSet);

        // Then
        assertThat(satisfied).isEqualTo(expectedResult);
    }

    @Test
    void expectRecordAbsence_withDuplicateRow() {
        // Given
        final ResultSet resultSet = new ResultSet(Arrays.asList(RECORD_1_INITIAL, RECORD_1_UPDATED));
        final ExpectRecordAbsence expectation = ExpectRecordAbsence.create(RECORD_1_INITIAL);

        // When + Then
        assertThatThrownBy(() -> expectation.isSatisfied(resultSet)).isInstanceOf(RuntimeException.class);
    }

    @ParameterizedTest
    @MethodSource
    void expectOrCondition(Or expectation, ResultSet resultSet, boolean expectedResult) {
        // When
        final boolean satisfied = expectation.isSatisfied(resultSet);

        // Then
        assertThat(satisfied).isEqualTo(expectedResult);
    }

    static Stream<Arguments> expectRecordPresence() {
        final ResultSet resultSet = new ResultSet(Arrays.asList(RECORD_1_INITIAL, RECORD_2_INITIAL));

        return Stream.of(
                Arguments.of(RECORD_1_INITIAL, resultSet, true),
                Arguments.of(RECORD_1_UPDATED, resultSet, false),
                Arguments.of(RECORD_2_INITIAL, resultSet, true),
                Arguments.of(RECORD_2_UPDATED, resultSet, false)
        );
    }

    static Stream<Arguments> expectRecordAbsence() {
        final ResultSet resultSet = new ResultSet(Arrays.asList(RECORD_1_INITIAL, RECORD_2_INITIAL));

        return Stream.of(
                Arguments.of(RECORD_1_INITIAL, resultSet, false),
                Arguments.of(RECORD_1_UPDATED, resultSet, false),
                Arguments.of(RECORD_2_INITIAL, resultSet, false),
                Arguments.of(RECORD_3_INITIAL, resultSet, true)
        );
    }

    static Stream<Arguments> expectOrCondition() {
        final ResultSet resultSet = new ResultSet(Arrays.asList(RECORD_1_INITIAL, RECORD_2_INITIAL));

        final ExpectRecordPresence initialRecord1Present = ExpectRecordPresence.create(RECORD_1_INITIAL);
        final ExpectRecordPresence updatedRecord1Present = ExpectRecordPresence.create(RECORD_1_UPDATED);
        final ExpectRecordAbsence record1Absent = ExpectRecordAbsence.create(RECORD_1_INITIAL);
        final ExpectRecordPresence initialRecord2Present = ExpectRecordPresence.create(RECORD_2_INITIAL);
        final ExpectRecordPresence updatedRecord2Present = ExpectRecordPresence.create(RECORD_2_UPDATED);

        return Stream.of(
                Arguments.of(initialRecord1Present.or(record1Absent), resultSet, true),
                Arguments.of(updatedRecord1Present.or(record1Absent), resultSet, false),
                Arguments.of(initialRecord2Present.or(updatedRecord2Present), resultSet, true)
        );
    }


}
