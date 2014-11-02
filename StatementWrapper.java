package org.eobjects.metamodel.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementWrapper implements Statement {

    private static final Logger logger = LoggerFactory.getLogger(StatementWrapper.class);
    protected Statement _statement;
    protected ConnectionWrapper _connectionWrapper;
    protected String _lastSql;

    public StatementWrapper(Statement statement, ConnectionWrapper connectionWrapper) {
        _statement = statement;
        _connectionWrapper = connectionWrapper;
    }

    public Statement getRealStatement() {
        return _statement;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _statement.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        ResultSetWrapper resultSetWrapper = new ResultSetWrapper(_statement.getResultSet(), this, sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return resultSetWrapper;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _statement.executeUpdate(sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public void close() throws SQLException {
        _statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return _statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        _statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return _statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        _statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        _statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return _statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        _statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        _statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return _statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        _statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        _statement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _statement.execute(sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return new ResultSetWrapper(_statement.getResultSet(), this, _lastSql);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return _statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return _statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        _statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return _statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        _statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return _statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return _statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return _statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        _statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        _statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int[] executeBatch = _statement.executeBatch();
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + _lastSql);
        return executeBatch;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return _connectionWrapper;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return _statement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new ResultSetWrapper(_statement.getResultSet(), this, _lastSql);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _statement.executeUpdate(sql, autoGeneratedKeys);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _statement.executeUpdate(sql, columnIndexes);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _statement.executeUpdate(sql, columnNames);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        long afterSQLExecution = System.currentTimeMillis();
        boolean execute = _statement.execute(sql, autoGeneratedKeys);
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _statement.execute(sql, columnIndexes);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _statement.execute(sql, columnNames);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return _statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return _statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        _statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return _statement.isPoolable();
    }
}