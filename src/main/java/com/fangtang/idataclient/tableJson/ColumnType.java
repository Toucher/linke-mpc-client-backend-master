package com.fangtang.idataclient.tableJson;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public enum ColumnType {
    INT,
    LONG,
    DATE,
    DECIMAL,
    LONGDECIMAL,
    STRING,
    BIT,
    BOOLEAN,
    TIMESTAMP,
    //    TINYINT,
//    SMALLINT,
//    MEDIUMINT,
    TIME,
    YEAR,
    DATETIME;


    @Override
    public String toString() {
        return super.toString();
    }

    public int getLength() {
        switch (this) {
            case INT:
                return 8;
            case DECIMAL:
                return 8;
            case LONG:
                return 8;
            case LONGDECIMAL:
                return 8;
            case DATE:
                return 8;
            case TIMESTAMP:
                return 8;
            case TIME:
                return 8;
            case YEAR:
                return 8;
            case DATETIME:
                return 8;
            case STRING:
                return 16;
            case BOOLEAN:
                return 1;
            case BIT:
                return 1;
            default:
                return 0;
        }
    }

    public int getBitLength() {
        switch (this) {
            case INT:
                return 32;
            case DECIMAL:
                return 32;
            case LONG:
                return 64;
            case LONGDECIMAL:
                return 64;
            case DATE:
                return 64;
            case TIMESTAMP:
                return 64;
            case TIME:
                return 64;
            case YEAR:
                return 64;
            case DATETIME:
                return 64;
            case STRING:
                return 128;
            case BOOLEAN:
                return 1;
            case BIT:
                return 1;
            default:
                return 0;
        }
    }

    public String columnExamplePayload() {
        switch (this) {
            case INT:
                return String.valueOf(1);
            case BIT:
                return String.valueOf(1);
            case BOOLEAN:
                return String.valueOf(1);
            case DECIMAL:
                return String.valueOf(1.);
            case LONG:
                return String.valueOf((long) 1 << 33);
            case LONGDECIMAL:
                return String.valueOf(((long) 1 << 33) + 1.);
            case DATE:
                SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
                Date date = new Date(0);
                return ft.format(date);
            case STRING:
                return "\"ABC\"";
            case TIMESTAMP:
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                Timestamp now = new Timestamp(System.currentTimeMillis());//获取系统当前时间

                return df.format(now);
            case DATETIME:
                SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                Timestamp now1 = new Timestamp(System.currentTimeMillis());//获取系统当前时间

                return df1.format(now1);


            case YEAR:
                SimpleDateFormat yf = new SimpleDateFormat("yyyy");
                Date date2 = new Date(0);
                return yf.format(date2);
            case TIME:
                SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
                Date date1 = new Date(0);
                return tf.format(date1);
            default:
                return String.valueOf(0);
        }
    }

    public String columnToMySQLType(boolean forParse) {
        if (forParse) {
            switch (this) {
                case INT:
                    return "bigint";
                case BIT:
                    return "bigint";
                case BOOLEAN:
                    return "bigint";
                case LONG:
                    return "bigint";
                case DECIMAL:
                    return "double";
                case LONGDECIMAL:
                    return "double";
                case DATE:
                    return "char(16)";
                case TIMESTAMP:
                    return "char(16)";
                case YEAR:
                    return "char(16)";
                case DATETIME:
                    return "char(16)";
                case STRING:
                    return "char(16)";
                case TIME:
                    return "char(16)";
                default:
                    return String.valueOf(0);
            }
        } else {
            switch (this) {
                case INT:
                    return "int";
                case BIT:
                    return "bit";
                case BOOLEAN:
                    return "bit";
                case DECIMAL:
                    return "float";
                case LONG:
                    return "bigint";
                case LONGDECIMAL:
                    return "double";
                case DATE:
                    return "date";
                case STRING:
                    return "char(16)";
                case TIMESTAMP:
                    return "timestamp";
                case TIME:
                    return "time";
                case DATETIME:
                    return "datetime";
                case YEAR:
                    return "year";
                default:
                    return String.valueOf(0);
            }
        }
    }
}
