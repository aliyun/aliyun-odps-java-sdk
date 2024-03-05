package com.aliyun.odps;

/**
 * SchemaFilter is used as a filter condition when querying all schemas
 *
 * name is the prefix of the schema name
 *
 */
public class SchemaFilter {

    private String name;

    private String owner;

    /**
     * Set the prefix of the schema name
     *
     * @param name - the prefix of the schema name when filtering
     *
     * */
    public void setName(String name) {this.name = name;}

    /**
     * get the prefix of the schema name
     *
     * @return the prefix of schema name
     */
    public String getName() {return name;}

    /**
     * Set the owner of the schema
     *
     * @param owner - the owner of the schema
     *
     * */
    public void setOwner(String owner) {this.owner = owner;}

    /**
     * get the owner of the schema
     *
     * @return the owner of the schema
     * */
    public String getOwner() {return owner;}


}
