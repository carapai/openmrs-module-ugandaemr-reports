package org.openmrs.module.ugandaemrreports;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.apache.lucene.queryparser.classic.ParseException;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.ugandaemrreports.common.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.openmrs.module.ugandaemrreports.reports.Helper.*;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.*;


/**
 * Created by carapai on 30/09/2017.
 */
public class MyTest {
    Date startDate = DateUtil.parseYmd("2017-01-01");
    Date lastDate = DateUtil.parseYmd("2017-03-31");

    @Test
    public void testSummarizeObs() throws IOException, ParseException, SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();
        summarizeObs(connection, "1900-01-01");
    }

    @Test
    public void testGetSummarizeObs() throws IOException, ParseException, SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();
        List<SummarizedObs> summarizedObs = getSummarizedObs(connection, "yq < 20171 and concept = 'encounter' and encounter_type = '14'");
        List<Data> d1 = reduceSummarizedObs(summarizedObs);
        List<Data> d2 = intersection(d1, d1);
//        Map<String, Long> ageGroups = summarize(reduceSummarizedObs(summarizedObs), get106);
//        System.out.println(ageGroups);
    }

    @Test
    public void testDatabaseSearch() throws IOException, ParseException, SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();

        List<String> zeros106 = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");
        List<String> zerosFemales = Arrays.asList("f", "h");
        List<String> zerosPreArt = Arrays.asList("a", "b");

        Integer summaryEncounterType = 14;
        Integer encounterEncounterType = 15;

        String quarter = getObsPeriod2(startDate, Enums.Period.QUARTERLY);

        LocalDate date = StubDate.dateOf(startDate);
        LocalDate endDate = StubDate.dateOf(lastDate);

        List<LocalDate> q1 = Periods.subtractQuarters(date, 2);
        List<LocalDate> q2 = Periods.subtractQuarters(date, 4);
        List<LocalDate> q3 = Periods.subtractQuarters(date, 8);
        List<LocalDate> q4 = Periods.subtractQuarters(date, 12);
        List<LocalDate> q5 = Periods.subtractQuarters(date, 16);
        List<LocalDate> q6 = Periods.subtractQuarters(date, 20);
        List<LocalDate> q7 = Periods.subtractQuarters(date, 24);

        Integer q = Integer.valueOf(quarter);

        String encounterQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter, "concept = 'encounter'");
        String inhQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter, "concept IN('99604','99605')", "grouped_by = 1");
        String artStartQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "concept = '99161'", "grouped_by = 2", "yq <= " + quarter);
        String sectionBConcepts = joinQuery(Enums.UgandaEMRJoiner.AND, "concept IN('death','99071','99072','99603','99160','90206','90306','99165','90211','5240','90209','99132','99084','99085','5497','730')", "yq <= " + quarter);

        List<SummarizedObs> encounters = getSummarizedObs(connection, encounterQuery);
        List<SummarizedObs> conceptsThisQuarter = getSummarizedObs(connection, "yq = " + quarter);
        List<SummarizedObs> artStart = getSummarizedObs(connection, artStartQuery);
        List<SummarizedObs> inh = getSummarizedObs(connection, inhQuery);
        List<SummarizedObs> one06B = getSummarizedObs(connection, sectionBConcepts);


        List<SummarizedObs> transferInB4Art = filter(conceptsThisQuarter, and(hasConcepts("99110"), hasGroup(1)));
        List<SummarizedObs> transferInOnArt = filter(conceptsThisQuarter, and(hasConcepts("99160"), hasGroup(2)));

        List<Data> startedArtThisQ = filterAndReduce(artStart, inTheQ(q));
        List<Data> startedArtB4Q = filterAndReduce(artStart, be4Q(q));
        List<Data> startedArtOnOrB4Q = reduceSummarizedObs(artStart);

        List<Data> artRegimen = filterAndReduce(conceptsThisQuarter, hasConcepts("90315"));
        List<Data> pregnantOrLactatingAtArtStart = filterAndReduce(conceptsThisQuarter, hasConcepts("99072", "99603"));
        List<Data> inhThisQ = filterAndReduce(inh, inTheQ(q));
        List<Data> inhBe4Q = filterAndReduce(inh, be4Q(q));
        List<Data> pregnantThisQuarter = filterData(filterAndReduce(conceptsThisQuarter, hasConcepts("90041", "90012")), hasAnswers("90003", "1065"));
        List<Data> entryPoint = filterAndReduce(conceptsThisQuarter, hasConcepts("90200"), hasAnswers("90012"));
        List<Data> goodAdherence = filterAndReduce(conceptsThisQuarter, hasConcepts("90221"), hasAnswers("90156"));

        List<SummarizedObs> allTransferIn = joinSummarizedObs(transferInB4Art, transferInOnArt);
        List<Data> transferIn = reduceSummarizedObs(allTransferIn);
        List<Data> summaryBe4Q = filterAndReduce(encounters, and(be4Q(q), hasEncounterType(String.valueOf(summaryEncounterType))));
        List<Data> summaryInTheQ = filterAndReduce(encounters, and(inTheQ(q), hasEncounterType(String.valueOf(summaryEncounterType))));
        List<Data> encounterInTheQ = filterAndReduce(encounters, and(inTheQ(q), hasEncounterType(String.valueOf(summaryEncounterType), String.valueOf(encounterEncounterType))));

        List<Data> withoutTransferIn = subtract(summaryInTheQ, transferIn);
        List<Data> withTransferIn = intersection(summaryInTheQ, transferIn);
        List<Data> mothers = intersection(withoutTransferIn, entryPoint);
        List<Data> cumulativeEnrolled = combine(summaryBe4Q, withoutTransferIn);
        List<Data> startedInh = intersection(withoutTransferIn, subtract(inhThisQ, inhBe4Q));
        Multimap<Integer, Integer> patientsFirstEncounters = getFirstEncounters(connection, Joiner.on(",").join(reduceData(withoutTransferIn)));
        Collection<Integer> firstEncounter = patientsFirstEncounters.get(encounterEncounterType);

        List<Data> pregnantFirstEncounter = filterData(pregnantThisQuarter, hasEncounter(firstEncounter));
        List<Data> allPregnant = combine(mothers, pregnantFirstEncounter);

        List<Data> cptThisQuarter = filterAndReduce(conceptsThisQuarter, hasConcepts("99037", "99033"));
        List<Data> tbThisQuarter = filterAndReduce(conceptsThisQuarter, hasConcepts("90216"));
        List<Data> tbDiagnosedThisQuarter = filterData(tbThisQuarter, hasAnswers("90078"));
        List<Data> startedTbThisQuarter = filterAndReduce(conceptsThisQuarter, and(hasConcepts("90217"), hasGroup(2)));
        List<Data> assessed4Mal = filterAndReduce(conceptsThisQuarter, hasConcepts("90236", "5090", "99030", "460", "68"));
        List<Data> malnutrition = filterAndReduce(conceptsThisQuarter, hasConcepts("68"), hasAnswers("99271", "99272", "99273"));
        List<Data> eligibleAndReady = filterAndReduce(conceptsThisQuarter, and(hasConcepts("90299"), hasGroup(2)));
        List<Data> artBasedOnCD4 = filterAndReduce(conceptsThisQuarter, hasConcepts("99082"));
        List<Data> onPreArt = subtract(encounterInTheQ, startedArtOnOrB4Q);
        List<Data> onPreArtAndCpt = intersection(cptThisQuarter, onPreArt);
        List<Data> onPreArtAndTb = intersection(tbThisQuarter, onPreArt);
        List<Data> onPreArtAndDiagnosedTb = intersection(tbDiagnosedThisQuarter, onPreArt);
        List<Data> onPreArtAndStartedTb = intersection(startedTbThisQuarter, onPreArt);
        List<Data> onPreArtAssessedMalnutrition = intersection(assessed4Mal, onPreArt);
        List<Data> onPreArtMalnutrition = intersection(malnutrition, onPreArt);
        List<Data> onPreArtEligibleNotStarted = intersection(eligibleAndReady, onPreArt);
        List<Data> startedArtBasedOnCD4 = intersection(startedArtThisQ, artBasedOnCD4);

        String[] allFirst = {"99005", "99040", "99015", "99016", "99041", "90002", "99039", "99143", "99042"};
        String[] allSecond = {"99044", "99043", "99284", "99286", "99887", "99007", "99008", "99283", "99144", "99888"};
        String[] allFirstAndSecondChildren = {"99006", "99885", "99884"};
        String[] allSecondAndFirstChildren = {"99046"};
        String[] firstAndSecondChildren = {"163017"};

        List<Data> allFirstLine = filterData(artRegimen, hasAnswers(allFirst));
        List<Data> firstLineAdult = filterData(artRegimen, and1(afterAge(12), hasAnswers(allFirstAndSecondChildren)));
        List<Data> firstLineChildren1 = filterData(artRegimen, and1(beforeAge(13), hasAnswers(allSecondAndFirstChildren)));


        List<Data> firstLineChild = filterData(artRegimen, and1(beforeAge(13), hasAnswers("163017", "99040", "99885",
                "99884", "99005", "99006", "99015", "99016", "99046", "99041")));

        List<Data> allSecondLine = filterData(artRegimen, hasAnswers(allSecond));
        List<Data> secondLineAdult = filterData(artRegimen, and1(afterAge(12), hasAnswers(allSecondAndFirstChildren)));
        List<Data> secondLineChildren1 = filterData(artRegimen, and1(beforeAge(13), hasAnswers(allFirstAndSecondChildren)));

        List<Data> thirdLine = filterData(artRegimen, hasAnswers("162987", "162986"));


        Map<String, Long> enrolledB4Q = summarize(summaryBe4Q, get106, zeros106);
        Map<String, Long> enrolledInTheQ = summarize(withoutTransferIn, get106, zeros106);
        Map<String, Long> pregnantAndLactating = summarize(allPregnant, pregnant, zerosFemales);
        Map<String, Long> inhInQ = summarize(startedInh, get106, zeros106);
        Map<String, Long> cumulative = summarize(cumulativeEnrolled, get106, zeros106);
        Map<String, Long> transferIns = summarize(withTransferIn, get106, zeros106);
        Map<String, Long> preArt = summarize(onPreArt, pre, zerosPreArt);
        Map<String, Long> preArtWithCPT = summarize(onPreArtAndCpt, pre, zerosPreArt);

        Map<String, Long> cumulativeOnArt = summarize(startedArtB4Q, get106, zeros106);
        Map<String, Long> startedArt = summarize(startedArtThisQ, get106, zeros106);
        Map<String, Long> startedArtCD4 = summarize(startedArtBasedOnCD4, get106, zeros106);
        Map<String, Long> startedArtPregnant = summarize(pregnantOrLactatingAtArtStart, pregnant, zerosFemales);
        Map<String, Long> onArt = summarize(startedArtOnOrB4Q, get106, zeros106);
        Map<String, Long> l1 = summarize(combine(allFirstLine, firstLineAdult, firstLineChildren1), get106, zeros106);
        Map<String, Long> l2 = summarize(combine(allSecondLine, secondLineAdult, secondLineChildren1), get106, zeros106);
        Map<String, Long> l3 = summarize(thirdLine, get106, zeros106);
        Map<String, Long> cpt = summarize(intersection(cptThisQuarter, artRegimen), get106, zeros106);


        Map<String, Object> data1 = get106B(q1, artStart, encounters, one06B, endDate);
        Map<String, Object> data2 = get106B(q2, artStart, encounters, one06B, endDate);
        Map<String, Object> data3 = get106B(q3, artStart, encounters, one06B, endDate);
        Map<String, Object> data4 = get106B(q4, artStart, encounters, one06B, endDate);
        Map<String, Object> data5 = get106B(q5, artStart, encounters, one06B, endDate);
        Map<String, Object> data6 = get106B(q6, artStart, encounters, one06B, endDate);
        Map<String, Object> data7 = get106B(q7, artStart, encounters, one06B, endDate);


        System.out.println(groupByPerson(startedArtOnOrB4Q).size());
        System.out.println(groupByPerson(encounterInTheQ).size());
        System.out.println(groupByPerson(subtract(encounterInTheQ, startedArtOnOrB4Q)).size());


    }

    @Test
    public void testTable() {
        Table<Integer, String, Integer> universityCourseSeatTable
                = ImmutableTable.<Integer, String, Integer>builder()
                .put(120, "M", 25)
                .put(234, "M", 25)
                .put(121, "F", 22)
                .put(344, "M", 82)
                .build();

        System.out.println(universityCourseSeatTable.columnMap());
    }

    @Test
    public void testJoda() {
        System.out.println(getObsPeriod2(DateUtil.parseYmd("2010-01-01"), Enums.Period.MONTHLY));
    }

    @Test
    public void test106A1B() throws SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();

        /*List<SummarizedObs> encounters = getSummarizedObs(connection, encounterSummary());
        List<SummarizedObs> allSummaryObs = getAllSummaryObservations(connection, encounters, startDate);
        makeCohort(allSummaryObs, startDate, 2);*/
    }

}
