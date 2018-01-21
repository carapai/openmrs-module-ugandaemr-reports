package org.openmrs.module.ugandaemrreports.reports;

import org.omg.CORBA.PUBLIC_MEMBER;
import org.openmrs.module.ugandaemrreports.common.Data;
import org.openmrs.module.ugandaemrreports.common.SummarizedEncounter;
import org.openmrs.module.ugandaemrreports.common.SummarizedObs;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Created by carapai on 01/11/2017.
 */
public class Predicates {


    public static Predicate<SummarizedObs> be4Q(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) < 0;
    }

    public static Predicate<SummarizedEncounter> be4EQ(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) < 0;
    }

    public static Predicate<SummarizedObs> onOrBe4Q(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) <= 0;
    }

    public static Predicate<SummarizedEncounter> onOrBe4Q1(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) <= 0;
    }

    public static Predicate<SummarizedObs> inTheQ(Integer quarter) {
        return p -> Objects.equals(p.getYq(), quarter);
    }

    public static Predicate<SummarizedEncounter> inTheEQ(Integer quarter) {
        return p -> Objects.equals(p.getYq(), quarter);
    }

    public static Predicate<SummarizedObs> onOrAfterQ(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) >= 0;
    }

    public static Predicate<SummarizedEncounter> onOrAfterQ1(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) >= 0;
    }

    public static Predicate<SummarizedObs> afterQ(Integer quarter) {
        return p -> p.getYq().compareTo(quarter) > 0;
    }

    public static Predicate<SummarizedObs> be4M(Integer month) {
        return p -> p.getYm().compareTo(month) < 0;
    }

    public static Predicate<SummarizedObs> onOrBe4M(Integer month) {
        return p -> p.getYm().compareTo(month) <= 0;
    }

    public static Predicate<SummarizedObs> inTheM(Integer month) {
        return p -> Objects.equals(p.getYm(), month);
    }

    public static Predicate<SummarizedObs> onOrAfterM(Integer month) {
        return p -> p.getYm().compareTo(month) >= 0;
    }

    public static Predicate<SummarizedObs> afterM(Integer month) {
        return p -> p.getYm().compareTo(month) > 0;
    }

    public static Predicate<SummarizedObs> hasEncounterType(Integer... encounterTypes) {
        return p -> Arrays.asList(encounterTypes).contains(p.getEncounterType());
    }

    public static Predicate<SummarizedEncounter> hasEncounterType2(Integer... encounterTypes) {
        return p -> Arrays.asList(encounterTypes).contains(p.getEncounterType());
    }

    public static Predicate<SummarizedObs> hasConcepts(Integer... concepts) {
        return p -> Arrays.asList(concepts).contains(p.getConcept());
    }

    public static Predicate<Data> hasConcepts1(Integer... concepts) {
        return p -> Arrays.asList(concepts).contains(p.getConcept());
    }

    public static Predicate<Data> hasNoConcepts1(Integer... concepts) {
        return p -> !Arrays.asList(concepts).contains(p.getConcept());
    }

    public static Predicate<SummarizedObs> hasVal(String... val) {
        return p -> Arrays.asList(val).contains(p.getVal());
    }

    public static Predicate<Data> hasVal1(String... val) {
        return p -> Arrays.asList(val).contains(p.getValue());
    }

    public static Predicate<Data> hasAnswers(String... answers) {
        return p -> Arrays.asList(answers).contains(p.getValue());
    }

    public static Predicate<Data> beforeAge(Integer age) {
        return p -> p.getAge().compareTo(age) < 0;
    }

    public static Predicate<Data> maxEncounter(Integer age) {
        return p -> p.getAge().compareTo(age) > 0;
    }

    public static Predicate<Data> afterAge(Integer age) {
        return p -> p.getAge().compareTo(age) > 0;
    }

    @SafeVarargs
    public static Predicate<SummarizedObs> and(Predicate<SummarizedObs>... predicates) {
        Predicate<SummarizedObs> p = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            p = p.and(predicates[i]);
        }
        return p;
    }
    @SafeVarargs
    public static Predicate<SummarizedEncounter> and2(Predicate<SummarizedEncounter>... predicates) {
        Predicate<SummarizedEncounter> p = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            p = p.and(predicates[i]);
        }
        return p;
    }

    @SafeVarargs
    public static Predicate<SummarizedObs> or(Predicate<SummarizedObs>... predicates) {
        Predicate<SummarizedObs> p = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            p = p.or(predicates[i]);
        }
        return p;
    }

    @SafeVarargs
    public static Predicate<Data> and1(Predicate<Data>... predicates) {
        Predicate<Data> p = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            p = p.and(predicates[i]);
        }
        return p;
    }

    @SafeVarargs
    public static Predicate<Data> or1(Predicate<Data>... predicates) {
        Predicate<Data> p = predicates[0];
        for (int i = 1; i < predicates.length; i++) {
            p = p.or(predicates[i]);
        }
        return p;
    }

    public static Predicate<Data> hasEncounter(Integer... encounters) {
        return p -> Arrays.asList(encounters).contains(p.getEncounterId());
    }

    public static Predicate<Data> hasPatient(Collection<Integer> patients) {
        return p -> patients.contains(p.getPatientId());
    }

    public static Predicate<Data> hasEncounter(Collection<Integer> encounters) {
        return p -> encounters.contains(p.getEncounterId());
    }

    public static Predicate<Data> min(String val) {
        return p -> p.getValue().compareTo(val) > 0;
    }

    public static Predicate<Data> max(String val) {
        return p -> p.getValue().compareTo(val) < 0;
    }

    public static Predicate<Data> eq(String val) {
        return p -> p.getValue().compareTo(val) == 0;
    }

    public static Predicate<Data> females() {
        return p -> p.getGender().compareTo("F") == 0;
    }

}
