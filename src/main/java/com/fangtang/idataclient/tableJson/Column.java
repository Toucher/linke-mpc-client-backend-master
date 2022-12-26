package com.fangtang.idataclient.tableJson;

//import com.ustc.linke.security.common.tools.CommonConversion;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.Math.pow;

public class Column {

    private String columnName;
    private int[] indeces;
    private final ColumnType columnType;
    private final int NUM_DECIMALS = 2;
    private final int NUM_LONG_DECIMALS = 2;
    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat yf = new SimpleDateFormat("yyyy");
    SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");

    public int startIndex;

    public Column(String columnName, int startIndex, ColumnType columnType){
        this.columnName = columnName;
        this.indeces = new int[columnType.getBitLength()];
        this.columnType = columnType;
        this.startIndex = startIndex;
        for(int i=0; i<columnType.getBitLength(); i++){
            this.indeces[i] = i + startIndex;
        }
    }

    public byte[] columnToBytes(String payload) throws NoSuchAlgorithmException, ParseException {
        switch (columnType) {
            case INT :
                int intPayload = Integer.parseInt(payload);
//                return CommonConversion.intToByteArray(intPayload);
            case LONG :
                long longPayload = Long.parseLong(payload);
//                return CommonConversion.longToBytes(longPayload, 8);
            case DECIMAL :
                float orgFloatPayload = Float.parseFloat(payload);
                int decimalPayload = (int) (orgFloatPayload * pow(10, NUM_DECIMALS));
//                return CommonConversion.intToByteArray(decimalPayload);
            case LONGDECIMAL :
                double orgDoublePayload = Double.parseDouble(payload);
                long longDecimalPayload = (long) (orgDoublePayload * pow(10, NUM_LONG_DECIMALS));
//                return CommonConversion.longToBytes(longDecimalPayload, 8);
            case STRING :
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(payload.getBytes());
                byte[] md5Payload = md.digest();
                //System.out.println(new BigInteger(md5Payload));
                return md5Payload;
            case DATE :
                Date datePayload = ft.parse(payload);
                long longDatePayload = datePayload.getTime();
//                return CommonConversion.longToBytes(longDatePayload, 8);
            case TIME :
                Date datePayload1 = ft.parse(payload);
                long longDatePayload1 = datePayload1.getTime();
//                return CommonConversion.longToBytes(longDatePayload, 8);
            case  YEAR :
                Date datePayload2 = ft.parse(payload);
                long longDatePayload2 = datePayload2.getTime();
//                return CommonConversion.longToBytes(longDatePayload, 8);
            case  DATETIME :
                Date datePayload3 = ft.parse(payload);
                long longDatePayload3 = datePayload3.getTime();
//                return CommonConversion.longToBytes(longDatePayload, 8);
            case BOOLEAN :
                boolean orgBoolPayload = Boolean.parseBoolean(payload);
                byte[] boolPayload = new byte[1];
                boolPayload[0] = (byte) (orgBoolPayload ? 1 : 0);
                return boolPayload;
            case BIT :
                int intPayload4 = Integer.parseInt(payload);
                byte[] boolPayload4 = new byte[1];
                boolPayload4[0] = (byte) (intPayload4==1 ? 1 : 0);
                return boolPayload4;
            case TIMESTAMP :
                Timestamp timestampPayload = Timestamp.valueOf(payload);
                long longTimestamp = timestampPayload.getTime();
//                return CommonConversion.longToBytes(longTimestamp, 8);
            default :
                return null;
        }
    }

    public int[] returnIndeces(){
        if(this.indeces[columnType.getBitLength() - 1] == 0){
            for(int i=0; i<columnType.getBitLength(); i++){
                this.indeces[i] = i + startIndex;
            }
        }
        return indeces;
    }

    public int getStartIndex(){
        return startIndex;
    }

    public int getColumnByteLength(){
        return columnType.getLength();
    }

    public String getColumnStringFromBytes(byte[] payload){
//        switch (columnType) {
//            case INT -> {
//                System.out.println(Arrays.toString(payload));
//                int intPayload = CommonConversion.byteArrayToInt(payload);
//                return intPayload+"";
//            }
//            case LONG -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                return longPayload+"";
//            }
//            case DECIMAL -> {
//                int intPayload = CommonConversion.byteArrayToInt(payload);
//                float floatPayload = (float) (intPayload / pow(10, NUM_DECIMALS));
//                return floatPayload+"";
//            }
//            case LONGDECIMAL -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                double doublePayload = longPayload / pow(10, NUM_DECIMALS);
//                return doublePayload+"";
//            }
//            case STRING -> {
//                String retPayload = new BigInteger(payload).toString(16);
//                //System.out.println(new BigInteger(md5Payload));
//                return retPayload;
//            }
//            case DATE -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                Date datePayload = new Date(longPayload);
//                //System.out.println(longDatePayload);
//                return ft.format(datePayload);
//            }
//            case BOOLEAN, BIT -> {
//                byte bytePayload = payload[0];
//                return bytePayload+"";
//            }
//            case TIMESTAMP, DATETIME -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                Timestamp timestamp = new Timestamp(longPayload);
//                return df.format(timestamp);
//            }
//            case TIME -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                Date datePayload = new Date(longPayload);
//                return tf.format(datePayload);
//            }
//            case YEAR -> {
//                long longPayload = CommonConversion.byteArrayToLong(payload);
//                Date datePayload = new Date(longPayload);
//                return yf.format(datePayload);
//            }
//            default -> {
//                return null;
//            }
//        }
        return null;
    }

    public String getColumnName(){
        return columnName;
    }

    public String returnExamplePayload(){
        return columnType.columnExamplePayload();
    }

    public ColumnType getColumnType(){
        return columnType;
    }

}

