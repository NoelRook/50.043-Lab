package simpledb.common;

import simpledb.storage.DbFile;


class Table{
    private DbFile file;
    private String name;
    private String pkey;

    Table(DbFile file, String name, String pkeyField){
        this.file = file;
        this.name = name;
        this.pkey = pkeyField;
    }

    //getters
    public String getName(){
        return this.name;
    }

    public DbFile getDbFile(){
        return this.file;
    }
    
    public String getPrimaryKey(){
        return this.pkey;
    }


}