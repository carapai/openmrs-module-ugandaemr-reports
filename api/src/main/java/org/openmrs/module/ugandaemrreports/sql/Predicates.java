package org.openmrs.module.ugandaemrreports.sql;


import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

public class Predicates {

    public static Predicate<Result> eq(String column, String value) {
        return p -> p.get(column).compareTo(value) == 0;
    }

    public static Predicate<Result> l(String column, String value) {
        return p -> p.get(column).compareTo(value) < 0;
    }

    public static Predicate<Result> le(String column, String value) {
        return p -> p.get(column).compareTo(value) <= 0;
    }

    public static Predicate<Result> g(String column, String value) {
        return p -> p.get(column).compareTo(value) > 0;
    }

    public static Predicate<Result> ge(String column, String value) {
        return p -> p.get(column).compareTo(value) >= 0;
    }

    public static Predicate<Result> has(String column, String... values) { return p -> Arrays.asList(values).contains(p.get(column)); }

    public static Predicate<Result> has(String column, Collection<String> values) { return p -> values.contains(p.get(column)); }

    // Concept

    public static Predicate<Result> eq(String value) {
        return p -> p.get("concept").compareTo(value) == 0;
    }

    public static Predicate<Result> l(String value) {
        return p -> p.get("concept").compareTo(value) < 0;
    }

    public static Predicate<Result> le(String value) {
        return p -> p.get("concept").compareTo(value) <= 0;
    }

    public static Predicate<Result> g(String value) {
        return p -> p.get("concept").compareTo(value) > 0;
    }

    public static Predicate<Result> ge(String value) {
        return p -> p.get("concept").compareTo(value) >= 0;
    }

    public static Predicate<Result> has(String... values) { return p -> Arrays.asList(values).contains(p.get("concept")); }

    public static Predicate<Result> has(Collection<String> values) {
        return p -> values.contains(p.get("concept"));
    }
}
