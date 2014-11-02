package org.eobjects.metamodel.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

public class ConnectionWrapper implements Connection {

    protected Connection _connection;

    public ConnectionWrapper(Connection connection) {
        _connection = connection;
    }

    public Connection getRealConnection() {
        return _connection;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return _connection.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return _connection.isWrapperFor(iface);
    }

    public StatementWrapper createStatement() throws SQLException {
        return new StatementWrapper(_connection.createStatement(), this);
    }

    public PreparedStatementWrapper prepareStatement(String sql) throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql), this);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return _connection.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return _connection.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        _connection.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return _connection.getAutoCommit();
    }

    public void commit() throws SQLException {
        _connection.commit();
    }

    public void rollback() throws SQLException {
        _connection.rollback();
    }

    public void close() throws SQLException {
        _connection.close();
    }

    public boolean isClosed() throws SQLException {
        return _connection.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return _connection.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        _connection.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return _connection.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        _connection.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return _connection.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        _connection.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return _connection.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return _connection.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        _connection.clearWarnings();
    }

    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new StatementWrapper(_connection.createStatement(resultSetType, resultSetConcurrency), this);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql, resultSetType, resultSetConcurrency),
                this);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return _connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return _connection.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        _connection.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        _connection.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return _connection.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return _connection.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return _connection.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        _connection.rollback();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        _connection.releaseSavepoint(savepoint);
    }

    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new StatementWrapper(_connection.createStatement(resultSetType, resultSetConcurrency), this);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability), this);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return _connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql, autoGeneratedKeys), this);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql, columnIndexes), this);
    }

    public PreparedStatementWrapper prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new PreparedStatementWrapper(_connection.prepareStatement(sql, columnNames), this);
    }

    public Clob createClob() throws SQLException {
        return _connection.createClob();
    }

    public Blob createBlob() throws SQLException {
        return _connection.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return _connection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return _connection.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return _connection.isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        _connection.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        _connection.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return _connection.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return _connection.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return _connection.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return _connection.createStruct(typeName, attributes);
    }
}