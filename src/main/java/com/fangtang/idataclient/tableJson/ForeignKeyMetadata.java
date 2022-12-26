package com.fangtang.idataclient.tableJson;

public class ForeignKeyMetadata {
    public String FK_TABLE_NAME;
    public String FK_TABLE_SCHEM;
    public String FK_TABLE_CAT;
    public String[] FK_COLUMN_NAME;

    public String Ref_TABLE_NAME;
    public String Ref_TABLE_SCHEM;
    public String Ref_TABLE_CAT;
    public String[] Ref_COLUMN_NAME;

    public ForeignKeyMetadata(String FK_TABLE_NAME, String FK_TABLE_SCHEM, String FK_TABLE_CAT, String[] FK_COLUMN_NAME,
                              String Ref_TABLE_NAME, String Ref_TABLE_SCHEM, String Ref_TABLE_CAT, String[] Ref_COLUMN_NAME) {
        this.FK_TABLE_NAME = FK_TABLE_NAME;
        this.FK_TABLE_SCHEM = FK_TABLE_SCHEM;
        this.FK_TABLE_CAT = FK_TABLE_CAT;
        this.FK_COLUMN_NAME = FK_COLUMN_NAME;
        this.Ref_TABLE_NAME = Ref_TABLE_NAME;
        this.Ref_TABLE_SCHEM = Ref_TABLE_SCHEM;
        this.Ref_TABLE_CAT = Ref_TABLE_CAT;
        this.Ref_COLUMN_NAME = Ref_COLUMN_NAME;
    }
}
