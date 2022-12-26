package com.fangtang.idataclient.tableJson;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * *
 *
 * @Author Mr.JIA
 * @Date 2022/12/11 16:24
 **/
public class TableJson {

    private int numIndeces = 0;
    private int numColumns = 0;
    private int numRows = 0;

    private String tableName = "defaultTable";

    private Column[] columns;

    private long[][] payloadLong;

    private byte[][] payloadByte;

    public TableMetadata[] tableMetadatas;

    public boolean containPayload = false;

    public static String driverName = "com.mysql.cj.jdbc.Driver";

    public TableJson(){
        this.columns = null;
        this.payloadLong = null;
        this.payloadByte = null;
    }

    public TableJson(String tableName){
        this();
        this.tableName = tableName;
    }

    public TableJson(String serverName, String databaseName, String tableName, String userName, String passwd){
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        List<TableMetadata> tableMetadataList = new ArrayList<>();
        List<Column> columnList = new ArrayList<>();

        this.tableName = tableName;

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        try {
            Connection connection = DriverManager.getConnection(url, userName, passwd);
            String sql = "select * from " + tableName;
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            int startIndex = 0;
            for(int i=1; i<=columnCount; i++){
                String columnName = resultSetMetaData.getColumnName(i);
                String columnType = resultSetMetaData.getColumnTypeName(i);
                ColumnType columnTypeTmp = SQLProcess.mysqlType2ColType(columnType);
//                String columnClass = resultSetMetaData.getColumnClassName(i);
                System.out.println(columnType + " " + columnName);
                Column column = new Column(columnName, startIndex, columnTypeTmp);
                columnList.add(column);
                startIndex += columnTypeTmp.getBitLength();
            }
            columns = columnList.toArray(new Column[0]);
            loadPK_FK_FromSQL(serverName, databaseName, tableName, userName, passwd);
            numColumns = columns.length;
            numIndeces = startIndex;

            sql = "select count(*) from " + tableName;
            resultSet = statement.executeQuery(sql);
            resultSet.next();
            numRows = resultSet.getInt("count(*)");
//            System.out.println(numRows);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public TableJson(int numRows, int numIndeces, Column[] columns, long[][] payloadLong, byte[][] payloadByte){
        this.numIndeces = numIndeces;
        this.numRows = numRows;
        this.columns = columns;
        this.numColumns = columns.length;
        this.payloadLong = payloadLong;
        this.payloadByte = payloadByte;
    }

    public void generateExampleJsonFile(){
        try{
            String tableFileName = tableName + ".txt";
            PrintStream printStream = new PrintStream(tableFileName);
//            System.setOut(printStream);
            String payload = "{";
            for (int i=0; i<numColumns; i++){
                payload += "\"" + columns[i].getColumnName() + "\": ";
                payload += columns[i].returnExamplePayload();
                if (i<numColumns-1)
                    payload += ",";
            }
            payload += "}";
            printStream.println(payload);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void generateMySQLDatabase(){

    }
    public void generateExampleMySQLTable(String serverName, String databaseName, String userName, String passwd, String tableName){
        SQLProcess sqlProcess = new SQLProcess(serverName, databaseName, userName, passwd);
        sqlProcess.writeTable(tableName, columns);
    }

    // For Json

    public void setNumIndeces(int numIndeces) {
        this.numIndeces = numIndeces;
    }

    public int getNumIndeces() {
        return numIndeces;
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }

    public int getNumColumns() {
        return numColumns;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumRows() {
        return numRows;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
        this.numColumns = columns.length;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setPayloadLong(long[][] payloadLong) {
        this.payloadLong = payloadLong;
    }

    public long[][] getPayloadLong() {
        if(containPayload){
            return payloadLong;
        }
        else return null;
    }

    public void setPayloadByte(byte[][] payloadByte) {
        this.payloadByte = payloadByte;
    }

    public byte[][] getPayloadByte() {
        if(containPayload) {
            return payloadByte;
        }
        else return null;
    }

    public void setTableMetadatas(TableMetadata[] tableMetadatas) {
        this.tableMetadatas = tableMetadatas;
    }

    public TableMetadata[] getTableMetadatas() {
        return tableMetadatas;
    }

    public void setContainPayload(boolean containPayload) {
        this.containPayload = containPayload;
    }

    public boolean getContainPayload() {
        return containPayload;
    }

    public Column getColumn(String columnName){
        for(Column column : columns){
            //System.out.println(column.getColumnName() + " " + columnName);
            if(column.getColumnName().equals(columnName)){
                return column;
            }
        }
        return null;
    }

    public byte[][] getColumnPayloadByte(String columnName){
        byte[][] columnPayload;
        for(Column column : columns){
            //System.out.println(column.getColumnName() + " " + columnName);
            if(column.getColumnName().equals(columnName)){
                columnPayload = new byte[column.returnIndeces().length][payloadByte[0].length];
                //System.out.println("Matched indeces: " + Arrays.toString(column.getIndeces()));
                for(int i = 0; i<column.returnIndeces().length; i++){
                    System.arraycopy(payloadByte[column.returnIndeces()[i]], 0, columnPayload[i],
                            0, payloadByte[0].length);
                }
                return columnPayload;
            }
        }
        return null;
    }

    public long[][] getColumnPayloadLong(String columnName){
        long[][] columnPayload;
        for(Column column : columns){
            if(column.getColumnName().equals(columnName)){
                columnPayload = new long[column.returnIndeces().length][payloadLong[0].length];
                for(int i = 0; i<column.returnIndeces().length; i++){
                    System.arraycopy(payloadLong[column.returnIndeces()[i]], 0, columnPayload[i],
                            0, payloadLong[0].length);
                }
                return columnPayload;
            }
        }
        return null;
    }

    // end of Json processing

   /* public void writeTableFromCSV(String filename, boolean isSkipFirstLine, char separator) {
        try{
            if(columns != null || numIndeces == 0){
                FileReader fileReader = new FileReader(filename);
                CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
                CSVReader csvReader = new CSVReaderBuilder(fileReader)
                        .withCSVParser(parser)
                        .withSkipLines(isSkipFirstLine ? 1 : 0)
                        .build();
                List<String[]> allData = csvReader.readAll();
                payloadTransformer(allData);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }*/




    public void printColumn(String columnName){
//        byte[][] columnPayload = getColumnPayloadByte(columnName);
//        //System.out.println("Data size: " + columnPayload.length + " " + columnPayload[0].length);
//        System.out.println("Stored data size: " + columnPayload.length + " " + numRows);
//
//        NaiveJavaMatrix columnPayloadMat = new NaiveJavaMatrix(numRows, columnPayload.length, columnPayload, false,
//                true);
//        BitMatrix columnPayloadMatT = columnPayloadMat.transpose();
//        //System.out.println("Column shape: " + columnPayloadMatT.getColumn(0));
//        System.out.println("Payload of Column " + columnName + ":");
//        for(int i=0; i<numRows; i++){
//            System.out.println(columnPayloadMatT.getColumn(i));
//        }
    }

    public void printColumnLong(String columnName){
//        long[][] columnPayloadLong = getColumnPayloadLong(columnName);
//        byte[][] columnPayload = new byte[columnPayloadLong.length][payloadByte[0].length];
//        for(int i=0; i<columnPayloadLong.length; i++){
//            //columnPayload[i] = CommonConversion.longArrayToByteArray(columnPayloadLong[i]);
//            System.arraycopy(CommonConversion.longArrayToByteArray(columnPayloadLong[i]),
//                    columnPayloadLong[i].length * 8 - payloadByte[0].length, columnPayload[i],
//                    0, payloadByte[0].length);
//        }
//        System.out.println("Data size: " + columnPayloadLong.length + " " + columnPayloadLong[0].length);
//        System.out.println("Stored data size: " + columnPayload.length + " " + numRows);
//
//        NaiveJavaMatrix columnPayloadMat = new NaiveJavaMatrix(numRows, columnPayload.length, columnPayload, false,
//                true);
//        BitMatrix columnPayloadMatT = columnPayloadMat.transpose();
//        //System.out.println("Column shape: " + columnPayloadMatT.getColumn(0));
//        System.out.println("Payload of Column " + columnName + ":");
//        for(int i=0; i<numRows; i++){
//            byte[] printColumnPayload = new byte[getColumn(columnName).getColumnByteLength()];
//            byte[] rawPrintBytes = columnPayloadMatT.getColumnBytes(i);
//            System.arraycopy(rawPrintBytes, 0, printColumnPayload,
//                    printColumnPayload.length - rawPrintBytes.length, rawPrintBytes.length);
//            System.out.println(getColumn(columnName).getColumnStringFromBytes(printColumnPayload));
//            //System.out.println(Arrays.toString(columnPayloadMatT.getColumn(i).toByteArray()));
//        }
    }

    public void writeTableFromSQL(String serverName, String databaseName, String userName, String passwd, String tableName){
        try{
            this.tableName = tableName;
            SQLProcess sqlProcess = new SQLProcess(serverName, databaseName, tableName, userName, passwd);
            List<String[]> allData = sqlProcess.readTable(tableName, columns);
            tableMetadatas = sqlProcess.getTableMetadatas();
            payloadTransformer(allData);
            sqlProcess.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void loadPK_FK_FromSQL(String serverName, String databaseName, String tableName, String userName, String passwd){
        try{
            this.tableName = tableName;
            SQLProcess sqlProcess = new SQLProcess(serverName, databaseName, tableName, userName, passwd);
            tableMetadatas = sqlProcess.getTableMetadatas();
            sqlProcess.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void payloadTransformer(List<String[]> allData) throws NoSuchAlgorithmException, ParseException {
//        numRows = allData.size();
//
//        int tmpRowLength = 0;
//        int tmpColumnByteLength = 0;
//        numIndeces = 0;
//        for (Column column: columns){
//            tmpColumnByteLength = column.getColumnByteLength();
//            tmpRowLength += tmpColumnByteLength;
//            numIndeces += column.returnIndeces().length;
//        }
//
//        int[] retIndeces = new int[numIndeces];
//        int paddingLength = 0;
//        int tmpColumnBitLength = 0;
//        int rowCnt = 0;
//        for (Column column: columns){
//            tmpColumnByteLength = column.getColumnByteLength();
//            tmpColumnBitLength = column.returnIndeces().length;
//            paddingLength += tmpColumnByteLength * 8 - tmpColumnBitLength;
//            for (int i : column.returnIndeces()){
//                retIndeces[rowCnt] = paddingLength + i;
//                rowCnt += 1;
//            }
//        }
//
//        int currentindex = 0;
//        byte[] rowPayload = new byte[tmpRowLength];
//        //byte[][] RawPayloadByte = new byte[tmpRowLength][numColumns];
//        NaiveJavaMatrix rawPayloadByte = new NaiveJavaMatrix(tmpRowLength * 8, numRows, false, true);
//        payloadByte = new byte[numIndeces][(int)ceil(numRows / 8.)];
//        payloadLong = new long[numIndeces][(int)ceil(numRows / (8 * 8.))];
//        byte[][] payloadBytePadding = new byte[numIndeces][(int)ceil(numRows / 64.) * 8];
//        //NaiveJavaMatrix matrix = rawPayloadByte.transpose();
//        //matrix.getColumn(i)
//        int columnByteLength = 0;
//        rowCnt = 0;
//        for(String[] row: allData){
//            //System.out.println(Arrays.toString(row));
//            currentindex = 0;
//            for (int i = 0; i < columns.length; i++){
//                byte[] payload = columns[i].columnToBytes(row[i]);
//                columnByteLength = columns[i].getColumnByteLength();
//                System.arraycopy(payload, 0, rowPayload, currentindex, columnByteLength);
//                currentindex += columnByteLength;
//            }
//            //System.arraycopy(rowPayload, 0, RawPayloadByte[rowCnt], 0, tmpRowLength);
//            rawPayloadByte.setColumn(rowCnt, rowPayload);
//            rowCnt += 1;
//        }
//        BitMatrix rawPayloadByteT = rawPayloadByte.transpose();
//
//        rowCnt = 0;
//
//        //System.out.println("Returned number of rows: " + numIndeces);
//        //System.out.println("Indeces: " + Arrays.toString(retIndeces));
//
//        for(int i : retIndeces){
//            //System.out.println("Row counter: " + rowCnt);
//            System.arraycopy(rawPayloadByteT.getColumnBytes(i), 0, payloadByte[rowCnt], 0, (int)ceil(numRows / 8.));
//            rowCnt += 1;
//            //System.out.println(Arrays.toString(rawPayloadByteT.getColumnBytes(i)));
//        }
//        //System.out.println("Load data into byte array");
//        //System.out.println("Data shape: (" + payloadByte.length + ", " + payloadByte[0].length + ")");
//
//        //payloadLong = CommonConversion.byteArrayToLong(payloadByte);
//
//        int startPos = payloadBytePadding[0].length - payloadByte[0].length;
//        int payloadLength = payloadByte[0].length;
//        for(int i=0; i<numIndeces; i++){
//            System.arraycopy(payloadByte[i], 0, payloadBytePadding[i], startPos, payloadLength);
//            System.arraycopy(CommonConversion.byteArrayToLongArray(payloadBytePadding[i]), 0,
//                    payloadLong[i], 0, (int)ceil(numRows / (8 * 8.)));
//        }
//        //System.out.println("Load data into Long array");
//        //System.out.println("Data shape: (" + payloadLong.length + ", " + payloadLong[0].length + ")");
//    }
    }
}
