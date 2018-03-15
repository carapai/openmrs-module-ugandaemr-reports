package org.openmrs.module.ugandaemrreports.sql;

import com.google.common.base.Joiner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Q {

    private String query = "";

    private List<Result> results;

    private List<Result> filtered;

    public Q() {
    }

    public Q(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    public List<Result> getFiltered() {
        return filtered;
    }

    public void setFiltered(List<Result> filtered) {
        this.filtered = filtered;
    }

    public Q n(String column) {
        this.query = this.query + " AND " + column + " IS NULL";
        return this;
    }

    public Q on(String column) {
        this.query = this.query + " OR " + column + " IS NULL";
        return this;
    }

    public Q l(String column, String val) {
        this.query = this.query + " AND " + column + " < '" + val + "'";

        return this;
    }

    public Q le(String column, String val) {
        this.query = this.query + " AND " + column + " <= '" + val + "'";
        return this;
    }

    public Q g(String column, String val) {
        this.query = this.query + " AND " + column + " > '" + val + "'";
        return this;
    }

    public Q ge(String column, String val) {
        this.query = this.query + " AND " + column + " >= '" + val + "'";
        return this;
    }

    public Q b(String column, String val1, String val2) {
        this.query = this.query + " AND " + column + " BETWEEN '" + val1 + "' AND '" + val2 + "'";
        return this;
    }

    public Q e(String column, String val) {
        this.query = this.query + " AND " + column + " = '" + val + "'";
        return this;
    }

    public Q in(String column, Collection<String> vals) {
        String s = Joiner.on(",").join(vals.stream().map(e -> "'" + e + "'").collect(Collectors.toList()));
        this.query = this.query + " AND " + column + " IN (" + s + ")";
        return this;
    }

    public Q in(String column, String... vals) {
        String s = Joiner.on(",").join(Arrays.stream(vals).map(e -> "'" + e + "'").collect(Collectors.toList()));
        this.query = this.query + " AND " + column + " IN (" + s + ")";
        return this;
    }

    public Q ol(String column, String val) {
        this.query = this.query + " OR " + column + " < '" + val + "'";

        return this;
    }

    public Q ole(String column, String val) {
        this.query = this.query + " OR " + column + " <= '" + val + "'";
        return this;
    }

    public Q og(String column, String val) {
        this.query = this.query + " OR " + column + " > '" + val + "'";
        return this;
    }

    public Q oge(String column, String val) {
        this.query = this.query + " OR " + column + " >= '" + val + "'";
        return this;
    }

    public Q ob(String column, String val1, String val2) {
        this.query = this.query + " OR " + column + " BETWEEN '" + val1 + "' AND '" + val2 + "'";
        return this;
    }

    public Q oe(String column, String val) {
        this.query = this.query + " OR " + column + " = '" + val + "'";
        return this;
    }

    public Q oin(String column, Collection<String> vals) {
        String s = Joiner.on(",").join(vals.stream().map(e -> "'" + e + "'").collect(Collectors.toList()));
        this.query = this.query + " AND " + column + " IN (" + s + ")";
        return this;
    }

    public Q oin(String column, String... vals) {
        String s = Joiner.on(",").join(Arrays.stream(vals).map(e -> "'" + e + "'").collect(Collectors.toList()));
        this.query = this.query + " OR " + column + " IN (" + s + ")";
        return this;
    }

    public Q bracket() {
        this.query = "(" + this.query + ")";
        return this;
    }

    public List<Result> obs(List<String> columns) throws SQLException {
        return Queries.obs(this.query, columns);
    }

    public ResultSet obs() throws SQLException {
        return Queries.obs(this.query);
    }

    public Q filter(Predicate<Result> predicate) {
        this.filtered = Helper.filter(this.results, predicate);
        return this;
    }

    public Set<String> patients() throws SQLException {
        return Helper.groupMax(this.filtered).keySet();
    }

    public String toString() {
        return this.query;
    }
}
