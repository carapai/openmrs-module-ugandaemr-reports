package org.openmrs.module.ugandaemrreports.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;


public class DataElement {
    private String name;
    private Map<String, Predicate<SummarizedObs>> predicates = new HashMap<>();
    private Map<String, Predicate<Data>> aggregations = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Predicate<SummarizedObs>> getPredicates() {
        return predicates;
    }

    public void setPredicates(Map<String, Predicate<SummarizedObs>> predicates) {
        this.predicates = predicates;
    }

    public Map<String, Predicate<Data>> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Predicate<Data>> aggregations) {
        this.aggregations = aggregations;
    }
}
