package com.facebook.presto.jdbc;

import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;

import java.sql.Types;

import static java.util.Objects.requireNonNull;

public class ParameterInfo {

    private static final int VARCHAR_MAX = 1024 * 1024 * 1024;
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();

    private final int position;
    private final int parameterType;
    private final TypeSignature parameterTypeSignature;
    private final ColumnInfo.Nullable nullable;
    private final boolean signed;
    private final int precision;
    private final int scale;

    public ParameterInfo(
            int position,
            int parameterType,
            TypeSignature parameterTypeSignature,
            ColumnInfo.Nullable nullable,
            boolean signed,
            int precision,
            int scale)
    {
        this.position = position;
        this.parameterType = parameterType;
        this.parameterTypeSignature = requireNonNull(parameterTypeSignature, "parameterTypeName is null");
        this.nullable = requireNonNull(nullable, "nullable is null");
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
    }


    public static void setTypeInfo(ParameterInfo.Builder builder, TypeSignature type)
    {
        builder.setParameterType(getType(type));
        switch (type.toString()) {
            case "boolean":
                break;
            case "bigint":
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setScale(0);
                break;
            case "integer":
                builder.setSigned(true);
                builder.setPrecision(10);
                builder.setScale(0);
                break;
            case "smallint":
                builder.setSigned(true);
                builder.setPrecision(5);
                builder.setScale(0);
                break;
            case "tinyint":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "real":
                builder.setSigned(true);
                builder.setPrecision(9);
                builder.setScale(0);
                break;
            case "double":
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setScale(0);
                break;
            case "varchar":
                builder.setSigned(true);
                builder.setPrecision(VARCHAR_MAX);
                builder.setScale(0);
                break;
            case "varbinary":
                builder.setSigned(true);
                builder.setPrecision(VARBINARY_MAX);
                builder.setScale(0);
                break;
            case "time":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "time with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "timestamp":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "timestamp with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "date":
                builder.setSigned(true);
                builder.setScale(0);
                break;
            case "interval year to month":
                break;
            case "interval day to second":
                break;
            case "decimal":
                builder.setSigned(true);
                builder.setPrecision(type.getParameters().get(0).getLongLiteral().intValue());
                builder.setScale(type.getParameters().get(1).getLongLiteral().intValue());
                break;
        }
    }

    private static int getType(TypeSignatureParameter typeParameter)
    {
        switch (typeParameter.getKind()) {
            case TYPE:
                return getType(typeParameter.getTypeSignature());
            default:
                return Types.JAVA_OBJECT;
        }
    }

    private static int getType(TypeSignature type)
    {
        if (type.getBase().equals("array")) {
            return Types.ARRAY;
        }
        switch (type.getBase()) {
            case "boolean":
                return Types.BOOLEAN;
            case "bigint":
                return Types.BIGINT;
            case "integer":
                return Types.INTEGER;
            case "smallint":
                return Types.SMALLINT;
            case "tinyint":
                return Types.TINYINT;
            case "real":
                return Types.REAL;
            case "double":
                return Types.DOUBLE;
            case "varchar":
                return Types.LONGNVARCHAR;
            case "char":
                return Types.CHAR;
            case "varbinary":
                return Types.LONGVARBINARY;
            case "time":
                return Types.TIME;
            case "time with time zone":
                return Types.TIME;
            case "timestamp":
                return Types.TIMESTAMP;
            case "timestamp with time zone":
                return Types.TIMESTAMP;
            case "date":
                return Types.DATE;
            case "decimal":
                return Types.DECIMAL;
            default:
                return Types.JAVA_OBJECT;
        }
    }

    public String getParameterTypeName()
    {
        return parameterTypeSignature.toString();
    }

    public int getPosition() {
        return position;
    }

    public int getParameterType() {
        return parameterType;
    }

    public TypeSignature getParameterTypeSignature() {
        return parameterTypeSignature;
    }

    public ColumnInfo.Nullable getNullable() {
        return nullable;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    static class Builder
    {
        private int position;
        private int parameterType;
        private TypeSignature parameterTypeSignature;
        private ColumnInfo.Nullable nullable;
        private boolean signed;
        private int precision;
        private int scale;

        public ParameterInfo.Builder setPosition(int position)
        {
            this.position = position;
            return this;
        }

        public ParameterInfo.Builder setParameterType(int parameterType)
        {
            this.parameterType = parameterType;
            return this;
        }

        public ParameterInfo.Builder setParameterTypeSignature(TypeSignature parameterTypeSignature)
        {
            this.parameterTypeSignature = parameterTypeSignature;
            return this;
        }

        public ParameterInfo.Builder setNullable(ColumnInfo.Nullable nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public ParameterInfo.Builder setSigned(boolean signed)
        {
            this.signed = signed;
            return this;
        }

        public ParameterInfo.Builder setPrecision(int precision)
        {
            this.precision = precision;
            return this;
        }

        public ParameterInfo.Builder setScale(int scale)
        {
            this.scale = scale;
            return this;
        }

        public ParameterInfo build()
        {
            return new ParameterInfo(
                    position,
                    parameterType,
                    parameterTypeSignature,
                    nullable,
                    signed,
                    precision,
                    scale);
        }
    }

}
