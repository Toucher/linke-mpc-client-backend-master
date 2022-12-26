package com.fangtang.idataclient.tableJson;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TableMetadata {
    public String TABLE_NAME;
    public String TABLE_SCHEM;
    public String TABLE_CAT;
    public String[] PK_COLUMN_NAME;
    public ForeignKeyMetadata[] foreignKeyMetadata;

    public TableMetadata(DatabaseMetaData databaseMetaData, String TABLE_NAME, String TABLE_SCHEM, String TABLE_CAT) {
        this.TABLE_NAME = TABLE_NAME;
        this.TABLE_SCHEM = TABLE_SCHEM;
        this.TABLE_CAT = TABLE_CAT;

        try {
            ResultSet innerResultSet = databaseMetaData.getPrimaryKeys(TABLE_CAT, null, TABLE_NAME);
            List<String> innerPkColumnList = new ArrayList<>();

            HashMap<String, String[]> refTableColumnsMap = new HashMap<>();
            HashMap<String, String[]> refTableRefColumnsMap = new HashMap<>();

            while (innerResultSet.next()) {
                innerPkColumnList.add(innerResultSet.getString("COLUMN_NAME"));
            }
            PK_COLUMN_NAME = new String[innerPkColumnList.size()];
            innerPkColumnList.toArray(PK_COLUMN_NAME);
            System.out.println("PK_COLUMN_NAME " + Arrays.toString(PK_COLUMN_NAME));

            innerResultSet = databaseMetaData.getImportedKeys(TABLE_CAT, null, TABLE_NAME);
            while (innerResultSet.next()) {
                String refTableName = innerResultSet.getString("PKTABLE_NAME");
                String refTableSchem = innerResultSet.getString("PKTABLE_SCHEM");
                String refTableCat = innerResultSet.getString("PKTABLE_CAT");
                String refTableIndexString = refTableCat + "." + refTableSchem + "." + refTableName;

                String[] columnName = new String[1];
                columnName[0] = innerResultSet.getString("FKCOLUMN_NAME");
                String[] refColumnName = new String[1];
                refColumnName[0] = innerResultSet.getString("PKCOLUMN_NAME");

                refTableColumnsMap.merge(refTableIndexString, columnName, this::arrayMerge);
                refTableRefColumnsMap.merge(refTableIndexString, refColumnName, this::arrayMerge);
            }
            System.out.println(TABLE_NAME);
            System.out.println(refTableColumnsMap.keySet());

            List<ForeignKeyMetadata> foreignKeyMetadataList = new ArrayList<>();
            for (String key : refTableColumnsMap.keySet()) {
                String[] refTableMetadataSplit = key.split("\\.");
                //System.out.println(key);
                System.out.println(Arrays.toString(refTableColumnsMap.get(key)));
                System.out.println(Arrays.toString(refTableRefColumnsMap.get(key)) + "\n");
                ForeignKeyMetadata foreignKeyMetadataTmp = new ForeignKeyMetadata(TABLE_NAME, TABLE_SCHEM, TABLE_CAT,
                        refTableColumnsMap.get(key), refTableMetadataSplit[2], refTableMetadataSplit[1],
                        refTableMetadataSplit[0], refTableRefColumnsMap.get(key));
                foreignKeyMetadataList.add(foreignKeyMetadataTmp);
            }
            foreignKeyMetadata = new ForeignKeyMetadata[foreignKeyMetadataList.size()];
            foreignKeyMetadataList.toArray(foreignKeyMetadata);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public TableMetadata(String TABLE_NAME, String TABLE_SCHEM, String TABLE_CAT, String[] PK_COLUMN_NAME,
                         ForeignKeyMetadata[] foreignKeyMetadata) {
        this.TABLE_NAME = TABLE_NAME;
        this.TABLE_SCHEM = TABLE_SCHEM;
        this.TABLE_CAT = TABLE_CAT;
        this.PK_COLUMN_NAME = PK_COLUMN_NAME;
        this.foreignKeyMetadata = foreignKeyMetadata;
    }

    private String[] arrayMerge(String[] oldArray, String[] newArray) {
        String[] retArray = new String[oldArray.length + newArray.length];
        System.arraycopy(oldArray, 0, retArray, 0, oldArray.length);
        System.arraycopy(newArray, 0, retArray, oldArray.length, newArray.length);
        return retArray;
    }
}
