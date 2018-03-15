package org.openmrs.module.ugandaemrreports.sql;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

public class Helper {
	
	public static List<Result> filter(List<Result> documents, Predicate<Result> predicate) {
		return documents.stream().filter(predicate).collect(Collectors.toList());
	}
	
	public static Result max(String column, List<Result> documents) {
        return documents.stream().max(comparing(d -> d.get(column))).get();
    }
	
	public static Result min(String column, List<Result> documents) {
        return documents.stream().min(comparing(d -> d.get(column))).get();

    }
	
	public static Map<String, List<Result>> group(List<Result> documents, String column) {
        return documents.stream().collect(Collectors.groupingBy(d -> d.get(column)));
    }
	
	public static Map<String, List<Result>> group(List<Result> documents) {
        return documents.stream().collect(Collectors.groupingBy(d -> d.get("person")));
    }
	
	public static Map<String, Result> groupMax(List<Result> documents, String column) {
        return documents.stream().collect(
                Collectors.toMap(d -> d.get("patient"), Function.identity(), (Result d1, Result d2) -> d1.get(column).compareTo(d2.get(column)) > 0 ? d1 : d2));
    }
	
	public static Map<String, Result> groupMin(List<Result> documents, String column) {
        return documents.stream().collect(
                Collectors.toMap(d -> d.get("patient"), Function.identity(), (Result d1, Result d2) -> d1.get(column).compareTo(d2.get(column)) < 0 ? d1 : d2));
    }
	
	public static Map<String, Result> groupMax(List<Result> documents) {
        return documents.stream().collect(
                Collectors.toMap(d -> d.get("patient"), Function.identity(), (Result d1, Result d2) -> d1.get("encounter").compareTo(d2.get("encounter")) > 0 ? d1 : d2));
    }
	
	public static Map<String, Result> groupMin(List<Result> documents) {
        return documents.stream().collect(
                Collectors.toMap(d -> d.get("patient"), Function.identity(), (Result d1, Result d2) -> d1.get("encounter").compareTo(d2.get("encounter")) < 0 ? d1 : d2));
    }
	
	public static Set<String> get(List<Result> documents, String column) {
		return group(documents, column).keySet();
	}
	
	public static Map<String, Result> difference(List<Result> documents, List<Result> documents1, String column) {
		return Maps.difference(groupMax(documents, column), groupMax(documents1, column)).entriesOnlyOnLeft();
	}
	
	public static Map<String, Result> intersection(List<Result> documents, List<Result> documents1, String column) {
		return Maps.difference(groupMax(documents, column), groupMax(documents1, column)).entriesInCommon();
	}
	
	public static Map<String, Result> difference(List<Result> documents, List<Result> documents1) {
		return difference(documents, documents1, "encounter");
	}
	
	public static Map<String, Result> intersection(List<Result> documents, List<Result> documents1) {
		return intersection(documents, documents1, "encounter");
	}
	
	public void processQuery() {
		
	}
}
