package org.openmrs.module.ugandaemrreports.sql;

import java.util.HashMap;
import java.util.Map;

public class Result {
    private Map<String, String> row = new HashMap<>();

    public Result() {

    }

    public Result(Map<String, String> row) {
        this.row = row;
    }

    public Map<String, String> getRow() {
        return row;
    }

    public void setRow(Map<String, String> row) {
        this.row = row;
    }

    public String get(String column) {
        return row.get(column);
    }

    public void set(String column, String value) {
        this.row.put(column, value);
    }
}