package com.facebook.presto.jdbc;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PrestoParameterMetaData implements ParameterMetaData {

    private final List<ParameterInfo> parameterInfo;

    public PrestoParameterMetaData(List<ParameterInfo> parameterInfo) {
        this.parameterInfo = ImmutableList.copyOf(requireNonNull(parameterInfo, "columnInfo is null"));
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterInfo.size();
    }

    @Override
    public int isNullable(int param)
            throws SQLException
    {
        ColumnInfo.Nullable nullable = parameter(param).getNullable();
        switch (nullable) {
            case NO_NULLS:
                return parameterNoNulls;
            case NULLABLE:
                return parameterNullable;
            case UNKNOWN:
                return parameterNullableUnknown;
        }
        throw new SQLException("Unhandled nullable type: " + nullable);
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return parameter(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return parameter(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return parameter(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return parameter(param).getParameterType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return parameter(param).getParameterTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        // see javax.sql.rowset.RowSetMetaDataImpl
        switch (parameter(param).getParameterType()) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class.getName();
            case Types.TINYINT:
                return Byte.class.getName();
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.REAL:
                return Float.class.getName();
            case Types.FLOAT:
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return "byte[]";
            case Types.DATE:
                return Date.class.getName();
            case Types.TIME:
                return Time.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.BLOB:
                return Blob.class.getName();
            case Types.CLOB:
                return Clob.class.getName();
            case Types.ARRAY:
                return Array.class.getName();
        }
        return String.class.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private ParameterInfo parameter(int param)
            throws SQLException
    {
        if ((param <= 0) || (param > parameterInfo.size())) {
            throw new SQLException("Invalid parameter index: " + param);
        }
        return parameterInfo.get(param - 1);
    }

}
