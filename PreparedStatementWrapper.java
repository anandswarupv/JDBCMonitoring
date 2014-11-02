package org.eobjects.metamodel.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {

    private static final Logger logger = LoggerFactory.getLogger(PreparedStatementWrapper.class);
    protected PreparedStatement _preparedStatement;
    protected ConnectionWrapper _connectionWrapper;
    protected String _lastSql;

    public PreparedStatementWrapper(PreparedStatement preparedStament, ConnectionWrapper connectionWrapper) {
        super(preparedStament, connectionWrapper);
        _preparedStatement = preparedStament;
        _connectionWrapper = connectionWrapper;
    }

    public PreparedStatement getRealPreparedStatement() {
        return _preparedStatement;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        ResultSetWrapper resultSetWrapper = new ResultSetWrapper(_preparedStatement.getResultSet(), this, sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return resultSetWrapper;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _preparedStatement.executeUpdate(sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public void close() throws SQLException {
        _preparedStatement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return _preparedStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        _preparedStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return _preparedStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        _preparedStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        _preparedStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return _preparedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        _preparedStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        _preparedStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return _preparedStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        _preparedStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        _preparedStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _preparedStatement.execute(sql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return new ResultSetWrapper(_preparedStatement.getResultSet(), this, _lastSql);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return _preparedStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return _preparedStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        _preparedStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return _preparedStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        _preparedStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return _preparedStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return _preparedStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return _preparedStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        _lastSql = sql;
        _preparedStatement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        _preparedStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int[] executeBatch = _preparedStatement.executeBatch();
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
        return _preparedStatement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new ResultSetWrapper(_preparedStatement.getResultSet(), this, _lastSql);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _preparedStatement.executeUpdate(sql, autoGeneratedKeys);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _preparedStatement.executeUpdate(sql, columnIndexes);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _preparedStatement.executeUpdate(sql, columnNames);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return executeUpdate;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _preparedStatement.execute(sql, autoGeneratedKeys);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _preparedStatement.execute(sql, columnIndexes);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        _lastSql = sql;
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _preparedStatement.execute(sql, columnNames);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + sql);
        return execute;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return _preparedStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return _preparedStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        _preparedStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return _preparedStatement.isPoolable();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _preparedStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _preparedStatement.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        ResultSetWrapper resultSetWrapper = new ResultSetWrapper(_preparedStatement.getResultSet(), this, _lastSql);
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + _lastSql);
        return resultSetWrapper;
    }

    @Override
    public int executeUpdate() throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        int executeUpdate = _preparedStatement.executeUpdate();
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + _lastSql);
        return executeUpdate;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        _preparedStatement.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        _preparedStatement.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        _preparedStatement.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        _preparedStatement.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        _preparedStatement.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        _preparedStatement.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        _preparedStatement.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        _preparedStatement.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        _preparedStatement.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        _preparedStatement.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        _preparedStatement.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        _preparedStatement.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        _preparedStatement.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        _preparedStatement.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        _preparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        _preparedStatement.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        _preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        _preparedStatement.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        _preparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        _preparedStatement.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        long beforeSQLExecution = System.currentTimeMillis();
        boolean execute = _preparedStatement.execute();
        long afterSQLExecution = System.currentTimeMillis();
        logger.debug("Time: " + (afterSQLExecution - beforeSQLExecution) + "millis for SQL " + _lastSql);
        return execute;
    }

    @Override
    public void addBatch() throws SQLException {
        _preparedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        _preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        _preparedStatement.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        _preparedStatement.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        _preparedStatement.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        _preparedStatement.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return _preparedStatement.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        _preparedStatement.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        _preparedStatement.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        _preparedStatement.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        _preparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        _preparedStatement.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return _preparedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        _preparedStatement.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        _preparedStatement.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        _preparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        _preparedStatement.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        _preparedStatement.setClob(parameterIndex, reader, length);

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        _preparedStatement.setBlob(parameterIndex, inputStream, length);

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        _preparedStatement.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        _preparedStatement.setSQLXML(parameterIndex, xmlObject);

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        _preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        _preparedStatement.setAsciiStream(parameterIndex, x, length);

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        _preparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        _preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        _preparedStatement.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        _preparedStatement.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        _preparedStatement.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        _preparedStatement.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        _preparedStatement.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        _preparedStatement.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        _preparedStatement.setNClob(parameterIndex, reader);
    }
}