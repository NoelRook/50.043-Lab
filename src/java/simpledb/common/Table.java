package simpledb.common;

class Table{
    private DbFile;
    private name;
    private pkeyField;

    Table(DbFile file, String name, String pkeyField){
        this.DbFile = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    //getters
    getName(){
        return this.name;
    }

    getDbFile(){
        return this.Dbfile;
    }
    
    getPrimaryKey(){
        return this.pkeyField;
    }


}