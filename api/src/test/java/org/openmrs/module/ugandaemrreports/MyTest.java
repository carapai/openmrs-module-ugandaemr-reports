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
    Date startDate = DateUtil.parseYmd("2007-01-01");
    Date lastDate = DateUtil.parseYmd("2007-03-31");

    @Test
    public void testSummarizeObs() throws IOException, ParseException, SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();
        summarizeObs(connection, "1900-01-01");
    }

    @Test
    public void testDatabaseSearch() throws IOException, ParseException, SQLException, ClassNotFoundException {
        Connection connection = testSqlConnection();

        List<String> zeros106 = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");
        List<String> zerosFemales = Arrays.asList("f", "h");
        List<String> zerosPreArt = Arrays.asList("a", "b");

        Integer summaryEncounterType = 8;
        Integer encounterEncounterType = 9;

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


        String summaryEncounterQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter, "encounter_type = 8");
        String encounterQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter, "encounter_type = 9");

        String numericConcepts1 = "concept IN(99604,99604)";
        String dateConcepts1 = "concept IN(99161,90299)";

        String numericConcepts2 = "concept IN(99037,99033,90236,5090,99082)";
        String dateConcepts2 = "concept IN(99160,90217)";
        String codedConcepts2 = "concept IN(99110,90315,99072,99603,90041,90012,90200,90221,90216,99030,68,460)";

        String numericQueryB4Q = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                numericConcepts1, "encounter_type IN(8,9)");
        String dateQueryB4Q = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                dateConcepts1, "encounter_type IN(8,9)");

        String numericQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                numericConcepts2, "encounter_type IN(8,9)");

        String dateQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                dateConcepts2, "encounter_type IN(8,9)");
        String codedQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                codedConcepts2, "encounter_type IN(8,9)");
//        String sectionBConcepts = joinQuery(Enums.UgandaEMRJoiner.AND, "concept IN('5096','death','99071','99072','99603','99160','90206','90306','99165','90211','5240','90209','99132','99084','99085','5497','730')", "yq <= " + quarter);

        List<SummarizedEncounter> summaryEncountersBe4OrInTheQuarter = getSummarizedEncounters(connection, summaryEncounterQuery);
        List<SummarizedEncounter> encountersInTheQuarter = getSummarizedEncounters(connection, encounterQuery);

        List<SummarizedObs> numericObsBe4Q = getSummarizedObs(connection, "value_numeric", numericQueryB4Q);
        List<SummarizedObs> dateObsBe4Q = getSummarizedObs(connection, "value_datetime", dateQueryB4Q);

        List<SummarizedObs> numericObsQ = getSummarizedObs(connection, "value_numeric", numericQueryQ);
        List<SummarizedObs> dateObsQ = getSummarizedObs(connection, "value_datetime", dateQueryQ);
        List<SummarizedObs> codedObsQ = getSummarizedObs(connection, "value_coded", codedQueryQ);


        List<SummarizedObs> transferInB4Art = filter(codedObsQ, hasConcepts(99110));
        List<SummarizedObs> transferInOnArt = filter(dateObsQ, hasConcepts(99160));
        List<SummarizedObs> startedArtOnOrB4Q = filter(dateObsBe4Q, hasConcepts(99161));
        List<SummarizedObs> artRegimen = filter(codedObsQ, hasConcepts(90315));


        List<Data> startedArtThisQ = filterAndReduce(startedArtOnOrB4Q, inTheQ(q));
        List<SummarizedObs> startedArtB4Q = filter(startedArtOnOrB4Q, be4Q(q));

        List<SummarizedObs> pregnantOrLactatingAtArtStart = filter(codedObsQ, hasConcepts(99072, 99603));
        List<Data> inhThisQ = filterAndReduce(numericObsBe4Q, and(inTheQ(q), hasConcepts(99604, 99605)));
        List<Data> inhBe4Q = filterAndReduce(numericObsBe4Q, and(be4Q(q), hasConcepts(99604, 99605)));
        List<Data> pregnantThisQuarter = filterAndReduce(codedObsQ, and(hasConcepts(90041, 90012), hasVal("90003", "1065")));
        List<Data> entryPoint = filterAndReduce(codedObsQ, and(hasConcepts(90200), hasVal("90012")));
        List<SummarizedObs> goodAdherence = filter(codedObsQ, and(hasConcepts(90221), hasVal("90156")));

        List<SummarizedObs> allTransferIn = joinSummarizedObs(transferInB4Art, transferInOnArt);
        List<Data> transferIn = reduceSummarizedObs(allTransferIn);
        List<SummarizedEncounter> summaryBe4Q = filterEncounter(summaryEncountersBe4OrInTheQuarter, be4EQ(q));
        List<SummarizedEncounter> summaryInTheQ = filterEncounter(summaryEncountersBe4OrInTheQuarter, inTheEQ(q));

        List<Data> summaryPageInQuarter = reduceSummarizedEncounters(summaryInTheQ);


        List<Data> withoutTransferIn = subtract(summaryPageInQuarter, transferIn);
        List<Data> withTransferIn = intersection(summaryPageInQuarter, transferIn);
        List<Data> mothers = intersection(withoutTransferIn, entryPoint);
        List<Data> cumulativeEnrolled = combine(reduceSummarizedEncounters(summaryBe4Q), withoutTransferIn);
        List<Data> startedInh = intersection(withoutTransferIn, subtract(inhThisQ, inhBe4Q));
        Multimap<Integer, Integer> patientsFirstEncounters = getFirstEncounters(connection, Joiner.on(",").join(reduceData(withoutTransferIn)));
        Collection<Integer> firstEncounter = patientsFirstEncounters.get(encounterEncounterType);

        List<Data> pregnantFirstEncounter = filterData(pregnantThisQuarter, hasEncounter(firstEncounter));
        List<Data> allPregnant = combine(mothers, pregnantFirstEncounter);

        List<Data> cptThisQuarter = filterAndReduce(numericObsQ, hasConcepts(99037, 99033));
        List<SummarizedObs> tbThisQuarter = filter(codedObsQ, hasConcepts(90216));
        List<Data> tbDiagnosedThisQuarter = filterAndReduce(tbThisQuarter, hasVal("90078"));
        List<Data> startedTbThisQuarter = filterAndReduce(dateObsQ, hasConcepts(90217));
        List<Data> assessed4MalNumeric = filterAndReduce(numericObsQ, hasConcepts(90236, 5090));
        List<Data> assessed4MalCoded = filterAndReduce(codedObsQ, hasConcepts(99030, 460, 68));
        List<Data> assessed4Mal = combine(assessed4MalNumeric, assessed4MalCoded);
        List<Data> malnutrition = filterAndReduce(numericObsQ, and(hasConcepts(68), hasVal("99271", "99272", "99273")));
        List<Data> eligibleAndReady = filterAndReduce(dateObsQ, hasConcepts(90299));
        List<Data> artBasedOnCD4 = filterAndReduce(numericObsQ, hasConcepts(99082));
        List<Data> onPreArt = subtract(reduceSummarizedEncounters(encountersInTheQuarter), reduceSummarizedObs(startedArtOnOrB4Q));
        List<Data> onPreArtAndCpt = intersection(cptThisQuarter, onPreArt);
        List<Data> onPreArtAndTb = intersection(reduceSummarizedObs(tbThisQuarter), onPreArt);
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

        List<Data> allFirstLine = filterAndReduce(artRegimen, hasVal(allFirst));
        List<Data> firstLineAdult = filterData(filterAndReduce(artRegimen, hasVal(allFirstAndSecondChildren)), afterAge(12));
        List<Data> firstLineChildren1 = filterData(filterAndReduce(artRegimen, hasVal(allSecondAndFirstChildren)), beforeAge(13));


        List<Data> firstLineChild = filterData(filterAndReduce(artRegimen, hasVal("163017", "99040", "99885",
                "99884", "99005", "99006", "99015", "99016", "99046", "99041")), and1(beforeAge(13)));

        List<Data> allSecondLine = filterAndReduce(artRegimen, hasVal(allSecond));
        List<Data> secondLineAdult = filterData(filterAndReduce(artRegimen, hasVal(allSecondAndFirstChildren)), afterAge(12));
        List<Data> secondLineChildren1 = filterData(filterAndReduce(artRegimen, hasVal(allFirstAndSecondChildren)), beforeAge(13));

        List<Data> thirdLine = filterAndReduce(artRegimen, hasVal("162987", "162986"));


        Map<String, Long> enrolledB4Q = summarize(reduceSummarizedEncounters(summaryBe4Q), get106, zeros106);
        Map<String, Long> enrolledInTheQ = summarize(withoutTransferIn, get106, zeros106);
        Map<String, Long> pregnantAndLactating = summarize(allPregnant, pregnant, zerosFemales);
        Map<String, Long> inhInQ = summarize(startedInh, get106, zeros106);
        Map<String, Long> cumulative = summarize(cumulativeEnrolled, get106, zeros106);
        Map<String, Long> transferIns = summarize(withTransferIn, get106, zeros106);
        Map<String, Long> preArt = summarize(onPreArt, pre, zerosPreArt);
        Map<String, Long> preArtWithCPT = summarize(onPreArtAndCpt, pre, zerosPreArt);

        Map<String, Long> cumulativeOnArt = summarize(reduceSummarizedObs(startedArtB4Q), get106, zeros106);
        Map<String, Long> startedArt = summarize(startedArtThisQ, get106, zeros106);
        Map<String, Long> startedArtCD4 = summarize(startedArtBasedOnCD4, get106, zeros106);
        Map<String, Long> startedArtPregnant = summarize(reduceSummarizedObs(pregnantOrLactatingAtArtStart), pregnant, zerosFemales);
        Map<String, Long> onArt = summarize(reduceSummarizedObs(startedArtOnOrB4Q), get106, zeros106);
        Map<String, Long> l1 = summarize(combine(allFirstLine, firstLineAdult, firstLineChildren1), get106, zeros106);
        Map<String, Long> l2 = summarize(combine(allSecondLine, secondLineAdult, secondLineChildren1), get106, zeros106);
        Map<String, Long> l3 = summarize(thirdLine, get106, zeros106);
        Map<String, Long> cpt = summarize(intersection(cptThisQuarter, reduceSummarizedObs(artRegimen)), get106, zeros106);


       /* Map<Integer, List<Data>> pregnantWomen = groupByPerson(filterAndReduce(one06B, hasConcepts("99072", "99603"), hasAnswers("90003")));
        Map<Integer, List<Data>> baselineCD4 = groupByPerson(filterAndReduce(one06B, hasConcepts("99071"), afterAge(4)));
        Map<Integer, List<Data>> ti = groupByPerson(filterAndReduce(one06B, hasConcepts("99160", "90206")));
        Map<Integer, List<Data>> to = groupByPerson(filterAndReduce(one06B, hasConcepts("90306", "99165", "90211")));
        Map<Integer, List<Data>> stopped = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99084"))));
        Map<Integer, List<Data>> restarted = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99085"))));
        Map<Integer, List<Data>> dead = groupByPerson(filterAndReduce(one06B, hasConcepts("death")));
        List<SummarizedObs> cd4 = filter(one06B, hasConcepts("5497", "730"));
        List<SummarizedObs> visits = filter(one06B, and(hasConcepts("5096")));
        List<SummarizedObs> encounterEncounters = filter(encounters, hasEncounterType("15"));

        Map<String, Object> data1 = get106B(q1, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data2 = get106B(q2, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data3 = get106B(q3, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data4 = get106B(q4, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data5 = get106B(q5, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data6 = get106B(q6, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
        Map<String, Object> data7 = get106B(q7, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);*/
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
        String quarter = getObsPeriod2(startDate, Enums.Period.QUARTERLY);
        String dateConcepts1 = "concept IN(99161)";
        String dateQueryB4Q = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                dateConcepts1, "encounter_type IN(8)");

        List<SummarizedObs> dateObsBe4Q = getSummarizedObs(connection, "value_datetime", dateQueryB4Q);
        List<SummarizedObs> dead = getSummarizedObs(connection, "value_death", "yq <= " + quarter);

        List<SummarizedObs> all = joinSummarizedObs(dateObsBe4Q,dead);
        LocalDate date = StubDate.dateOf(startDate);

        List<LocalDate> q1 = Periods.subtractQuarters(date, 2);
        Map<String, Object> data = get1061BCohorts(lastDate, q1, all, connection);
        /*Set<Integer> concepts = new HashSet<>(Arrays.asList(5096, 99071, 99072, 99603, 99160, 90206, 90306, 99165, 90211, 5240, 90209, 99132, 99084, 99085, 5497, 730));
        Set<Integer> patients = new HashSet<>(Arrays.asList(193));
        Map<String, Set<Integer>> p = new HashMap<>();
        p.put("Started", patients);
        Set<Integer> encounterTypes = new HashSet<>(Arrays.asList(8, 9));
        Connection connection = testSqlConnection();
        CohortTracker cohortTracker = new CohortTracker();
        cohortTracker.setConnection(connection);
        cohortTracker.setConcepts(concepts);
        cohortTracker.setEndDate(DateUtil.parseYmd("2017-03-31"));
        cohortTracker.setPatients(p);
        cohortTracker.setEncounterTypes(encounterTypes);
        Map<String, List<Data>> data = cohortTracker.execute();*/
        /*List<SummarizedObs> encounters = getSummarizedObs(connection, encounterSummary());
        List<SummarizedObs> allSummaryObs = getAllSummaryObservations(connection, encounters, startDate);
        makeCohort(allSummaryObs, startDate, 2);*/
    }

}
