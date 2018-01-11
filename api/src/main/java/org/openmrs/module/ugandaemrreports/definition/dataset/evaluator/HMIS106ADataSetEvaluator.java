package org.openmrs.module.ugandaemrreports.definition.dataset.evaluator;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.joda.time.LocalDate;
import org.openmrs.annotation.Handler;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.MapDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.ugandaemrreports.common.*;
import org.openmrs.module.ugandaemrreports.definition.dataset.definition.HMIS106ADataSetDefinition;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

import static org.openmrs.module.ugandaemrreports.reports.Helper.*;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.*;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.hasAnswers;

/**
 */
@Handler(supports = {HMIS106ADataSetDefinition.class})
public class HMIS106ADataSetEvaluator implements DataSetEvaluator {

    @Override
    public MapDataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evaluationContext) throws EvaluationException {

        MapDataSet dataSet = new MapDataSet(dataSetDefinition, evaluationContext);
        HMIS106ADataSetDefinition definition = (HMIS106ADataSetDefinition) dataSetDefinition;

        List<String> zeros106 = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");
        List<String> zerosFemales = Arrays.asList("f", "h");
        List<String> zerosPreArt = Arrays.asList("a", "b");

        /*try {

            Connection connection = sqlConnection();
            Date startDate = definition.getStartDate();
            LocalDate date = StubDate.dateOf(startDate);
            LocalDate endDate = StubDate.dateOf(definition.getEndDate());

            List<LocalDate> q1 = Periods.subtractQuarters(date, 2);
            List<LocalDate> q2 = Periods.subtractQuarters(date, 4);
            List<LocalDate> q3 = Periods.subtractQuarters(date, 8);
            List<LocalDate> q4 = Periods.subtractQuarters(date, 12);
            List<LocalDate> q5 = Periods.subtractQuarters(date, 16);
            List<LocalDate> q6 = Periods.subtractQuarters(date, 20);
            List<LocalDate> q7 = Periods.subtractQuarters(date, 24);

            Integer summaryEncounterType = 14;
            Integer encounterEncounterType = 15;

            String quarter = getObsPeriod2(startDate, Enums.Period.QUARTERLY);

            Integer q = Integer.valueOf(quarter);

            String encounterQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter, "concept = 'encounter'", constructSQLInQuery("encounter_type", summaryEncounterType, encounterEncounterType));
            String inhQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter, "concept IN('99604','99605')", "grouped_by = 1");
            String artStartQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "concept = '99161'", "grouped_by = 2", "yq <= " + quarter);
            String eligibleQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "concept = '90299'", "grouped_by = 2", "yq <= " + quarter);
            String sectionBConcepts = joinQuery(Enums.UgandaEMRJoiner.AND, "concept IN('5096','death','99071','99072','99603','99160','90206','90306','99165','90211','5240','90209','99132','99084','99085','5497','730')", "yq <= " + quarter);


            List<SummarizedObs> encounters = getSummarizedObs(connection, encounterQuery);
            List<SummarizedObs> conceptsThisQuarter = getSummarizedObs(connection, "yq = " + quarter);
            List<SummarizedObs> artStart = getSummarizedObs(connection, artStartQuery);
            List<SummarizedObs> eligible = getSummarizedObs(connection, eligibleQuery);
            List<SummarizedObs> inh = getSummarizedObs(connection, inhQuery);
            List<SummarizedObs> one06B = getSummarizedObs(connection, sectionBConcepts);

            List<SummarizedObs> transferInB4Art = filter(conceptsThisQuarter, and(hasConcepts("99110"), hasGroup(1)));
            List<SummarizedObs> transferInOnArt = filter(conceptsThisQuarter, and(hasConcepts("99160"), hasGroup(2)));
            List<SummarizedObs> allTransferIn = joinSummarizedObs(transferInB4Art, transferInOnArt);

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
            List<Data> eligibleAndReady = reduceSummarizedObs(eligible);
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
            Map<String, Long> pregnantAndLactating = summarize(allPregnant, pregnant, zerosFemales, females());
            Map<String, Long> inhInQ = summarize(startedInh, get106, zeros106);
            Map<String, Long> cumulative = summarize(cumulativeEnrolled, get106, zeros106);
            Map<String, Long> transferIns = summarize(withTransferIn, get106, zeros106);
            Map<String, Long> preArt = summarize(onPreArt, pre, zerosPreArt);
            Map<String, Long> preArtWithCPT = summarize(onPreArtAndCpt, pre, zerosPreArt);

            Map<String, Long> cumulativeOnArt = summarize(startedArtB4Q, get106, zeros106);
            Map<String, Long> startedArt = summarize(startedArtThisQ, get106, zeros106);
            Map<String, Long> startedArtCD4 = summarize(startedArtBasedOnCD4, get106, zeros106);
            Map<String, Long> startedArtPregnant = summarize(pregnantOrLactatingAtArtStart, pregnant, zerosFemales, females());
            Map<String, Long> onArt = summarize(startedArtOnOrB4Q, get106, zeros106);
            Map<String, Long> l1 = summarize(combine(allFirstLine, firstLineAdult, firstLineChildren1), get106, zeros106);
            Map<String, Long> l2 = summarize(combine(allSecondLine, secondLineAdult, secondLineChildren1), get106, zeros106);
            Map<String, Long> l3 = summarize(thirdLine, get106, zeros106);
            Map<String, Long> cpt = summarize(intersection(cptThisQuarter, artRegimen), get106, zeros106);


            addData(enrolledB4Q, dataSet, "1");
            addData(enrolledInTheQ, dataSet, "2");
            addData(pregnantAndLactating, dataSet, "3");
            addData(inhInQ, dataSet, "4");
            addData(cumulative, dataSet, "5");
            addData(transferIns, dataSet, "6");
            addData(preArt, dataSet, "7");
            addData(preArtWithCPT, dataSet, "8");
            addData(onPreArtAndTb, dataSet, "i", "9");
            addData(onPreArtAndDiagnosedTb, dataSet, "i", "10");
            addData(onPreArtAndStartedTb, dataSet, "i", "11");
            addData(onPreArtAssessedMalnutrition, dataSet, "i", "12");
            addData(onPreArtMalnutrition, dataSet, "i", "13");
            addData(onPreArtEligibleNotStarted, dataSet, "i", "14");

            addData(cumulativeOnArt, dataSet, "15");
            addData(startedArt, dataSet, "16");
            addData(startedArtCD4, dataSet, "17");
            addData(startedArtPregnant, dataSet, "18");
            addData(onArt, dataSet, "19");
            addData(l1, dataSet, "20");
            addData(l2, dataSet, "21");
            addData(l3, dataSet, "22");
            addData(cpt, dataSet, "23");

            addData(intersection(tbThisQuarter, artRegimen), dataSet, "i", "24");
            addData(intersection(tbDiagnosedThisQuarter, artRegimen), dataSet, "i", "25");
            addData(intersection(startedTbThisQuarter, artRegimen), dataSet, "i", "26");
            addData(0, dataSet, "i", "27");
            addData(intersection(goodAdherence, artRegimen), dataSet, "i", "28");
            addData(intersection(assessed4Mal, artRegimen), dataSet, "i", "29");
            addData(intersection(malnutrition, artRegimen), dataSet, "i", "30");

            Map<Integer, List<Data>> pregnantWomen = groupByPerson(filterAndReduce(one06B, hasConcepts("99072", "99603"), hasAnswers("90003")));
            Map<Integer, List<Data>> baselineCD4 = groupByPerson(filterAndReduce(one06B, hasConcepts("99071"), afterAge(4)));
            Map<Integer, List<Data>> ti = groupByPerson(filterAndReduce(one06B, hasConcepts("99160", "90206")));
            Map<Integer, List<Data>> to = groupByPerson(filterAndReduce(one06B, hasConcepts("90306", "99165", "90211")));
            Map<Integer, List<Data>> stopped = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99084"), hasGroup(2))));
            Map<Integer, List<Data>> restarted = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99085"), hasGroup(2))));
            Map<Integer, List<Data>> dead = groupByPerson(filterAndReduce(one06B, hasConcepts("death")));
            List<SummarizedObs> cd4 = filter(one06B, hasConcepts("5497", "730"));
            List<SummarizedObs> visits = filter(one06B, and(hasConcepts("5096"), hasGroup(1)));
            List<SummarizedObs> encounterEncounters = filter(encounters, hasEncounterType("15"));

            Map<String, Object> data1 = get106B(q1, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data2 = get106B(q2, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data3 = get106B(q3, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data4 = get106B(q4, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data5 = get106B(q5, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data6 = get106B(q6, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);
            Map<String, Object> data7 = get106B(q7, artStart, pregnantWomen, baselineCD4, ti, to, stopped, restarted, dead, cd4, visits, encounterEncounters, endDate);

            for (Map.Entry<String, Object> data : data1.entrySet()) {
                String label = "1" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data2.entrySet()) {
                String label = "2" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data3.entrySet()) {
                String label = "3" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data4.entrySet()) {
                String label = "4" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data5.entrySet()) {
                String label = "5" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data6.entrySet()) {
                String label = "6" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

            for (Map.Entry<String, Object> data : data7.entrySet()) {
                String label = "7" + data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }

        } catch (Exception e) {
            System.out.println("Some silly error is there");
        }*/
        return dataSet;
    }
}