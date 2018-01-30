package org.openmrs.module.ugandaemrreports.definition.dataset.evaluator;

import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
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

import static org.openmrs.module.ugandaemrreports.reports.Helper.*;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.*;

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

        try {

            Connection connection = sqlConnection();
            Date startDate = definition.getStartDate();
            Date lastDate = definition.getEndDate();
            LocalDate date = StubDate.dateOf(startDate);
            String quarter = getObsPeriod2(startDate, Enums.Period.QUARTERLY);
            Integer q = Integer.valueOf(quarter);

            Integer summaryEncounterType = 8;
            Integer encounterEncounterType = 9;

            String allEncounters = Joiner.on("").join(Arrays.asList(summaryEncounterType, encounterEncounterType));

            List<LocalDate> allQuarters = Arrays.asList(
                    Periods.subtractQuarters(date, 2).get(1),
                    Periods.subtractQuarters(date, 4).get(1),
                    Periods.subtractQuarters(date, 8).get(1),
                    Periods.subtractQuarters(date, 12).get(1),
                    Periods.subtractQuarters(date, 16).get(1),
                    Periods.subtractQuarters(date, 20).get(1),
                    Periods.subtractQuarters(date, 24).get(1)
            );

            String encounterSummaryQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                    "encounter_type = " + String.valueOf(summaryEncounterType));
            String encounterQuery = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                    "encounter_type = " + String.valueOf(encounterEncounterType));

            String numericConcepts1 = "concept IN(99604,99604)";
            String dateConcepts1 = "concept IN(99161,90299)";

            String numericConcepts2 = "concept IN(99037,99033,90236,5090,99082)";
            String dateConcepts2 = "concept IN(99160,90217)";
            String codedConcepts2 = "concept IN(99110,90315,99072,99603,90041,90012,90200,90221,90216,99030,68,460)";

            String numericQueryB4Q = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                    numericConcepts1, "encounter_type IN(8,9)");
            String dateQueryB4Q = joinQuery(Enums.UgandaEMRJoiner.AND, "yq <= " + quarter,
                    dateConcepts1, String.format("encounter_type IN(%s)", allEncounters));

            String numericQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                    numericConcepts2, String.format("encounter_type IN(%s)", allEncounters));

            String dateQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                    dateConcepts2, String.format("encounter_type IN(%s)", allEncounters));
            String codedQueryQ = joinQuery(Enums.UgandaEMRJoiner.AND, "yq = " + quarter,
                    codedConcepts2, String.format("encounter_type IN(%s)", allEncounters));

            List<SummarizedObs> artEncounters = getSummarizedEncounters(connection, encounterQuery);
            List<SummarizedObs> summaryEncounters = getSummarizedEncounters(connection, encounterSummaryQuery);
            List<SummarizedObs> numericObsBe4Q = getSummarizedObs(connection, "value_numeric", numericQueryB4Q);
            List<SummarizedObs> dateObsBe4Q = getSummarizedObs(connection, "value_datetime", dateQueryB4Q);
            List<SummarizedObs> numericObsQ = getSummarizedObs(connection, "value_numeric", numericQueryQ);
            List<SummarizedObs> dateObsQ = getSummarizedObs(connection, "value_datetime", dateQueryQ);
            List<SummarizedObs> codedObsQ = getSummarizedObs(connection, "value_coded", codedQueryQ);
            List<SummarizedObs> deadPeople = getSummarizedObs(connection, "value_death", "yq <= " + quarter);

            List<SummarizedObs> transferInB4Art = filter(codedObsQ, hasConcepts(99110));
            List<SummarizedObs> transferInOnArt = filter(dateObsQ, hasConcepts(99160));
            List<SummarizedObs> startedArtOnOrB4Q = filter(dateObsBe4Q, hasConcepts(99161));
            List<SummarizedObs> artRegimen = filter(codedObsQ, hasConcepts(90315));
            List<SummarizedObs> pregnantOrLactatingAtArtStart = filter(codedObsQ, hasConcepts(99072, 99603));
            List<SummarizedObs> allTransferIn = joinSummarizedObs(transferInB4Art, transferInOnArt);
            List<SummarizedObs> startedArtB4Q = filter(startedArtOnOrB4Q, be4Q(q));
            List<SummarizedObs> summaryBe4Q = filter(summaryEncounters, be4Q(q));
            List<SummarizedObs> summaryInTheQ = filter(summaryEncounters, inTheQ(q));
            List<SummarizedObs> artInTheQ = filter(artEncounters, inTheQ(q));
            List<SummarizedObs> tbThisQuarter = filter(codedObsQ, hasConcepts(90216));

            List<Data> startedArtThisQ = filterAndReduce(startedArtOnOrB4Q, inTheQ(q));
            List<Data> inhThisQ = filterAndReduce(numericObsBe4Q, and(inTheQ(q), hasConcepts(99604, 99605)));
            List<Data> inhBe4Q = filterAndReduce(numericObsBe4Q, and(be4Q(q), hasConcepts(99604, 99605)));
            List<Data> pregnantThisQuarter = filterAndReduce(codedObsQ, and(hasConcepts(90041, 90012), hasVal("90003", "1065")));
            List<Data> entryPoint = filterAndReduce(codedObsQ, and(hasConcepts(90200), hasVal("90012")));
            List<Data> goodAdherence = filterAndReduce(codedObsQ, and(hasConcepts(90221), hasVal("90156")));
            List<Data> transferIn = reduceSummarizedObs(allTransferIn);
            List<Data> summaryPageInQuarter = reduceSummarizedObs(summaryInTheQ);
            List<Data> withoutTransferIn = subtract(summaryPageInQuarter, transferIn);
            List<Data> withTransferIn = intersection(summaryPageInQuarter, transferIn);
            List<Data> mothers = intersection(withoutTransferIn, entryPoint);
            List<Data> cumulativeEnrolled = combine(reduceSummarizedObs(summaryBe4Q), withoutTransferIn);
            List<Data> startedInh = intersection(withoutTransferIn, subtract(inhThisQ, inhBe4Q));

            Multimap<Integer, Integer> patientsFirstEncounters = getFirstEncounters(connection, Joiner.on(",").join(reduceData(withoutTransferIn)));
            Collection<Integer> firstEncounter = patientsFirstEncounters.get(encounterEncounterType);

            List<Data> pregnantFirstEncounter = filterData(pregnantThisQuarter, hasEncounter(firstEncounter));
            List<Data> allPregnant = combine(mothers, pregnantFirstEncounter);
            List<Data> cptThisQuarter = filterAndReduce(numericObsQ, hasConcepts(99037, 99033));
            List<Data> tbDiagnosedThisQuarter = filterAndReduce(tbThisQuarter, hasVal("90078"));
            List<Data> startedTbThisQuarter = filterAndReduce(dateObsQ, hasConcepts(90217));
            List<Data> assessed4MalNumeric = filterAndReduce(numericObsQ, hasConcepts(90236, 5090));
            List<Data> assessed4MalCoded = filterAndReduce(codedObsQ, hasConcepts(99030, 460, 68));
            List<Data> assessed4Mal = combine(assessed4MalNumeric, assessed4MalCoded);
            List<Data> malnutrition = filterAndReduce(numericObsQ, and(hasConcepts(68), hasVal("99271", "99272", "99273")));
            List<Data> eligibleAndReady = filterAndReduce(dateObsQ, hasConcepts(90299));
            List<Data> artBasedOnCD4 = filterAndReduce(numericObsQ, hasConcepts(99082));
            List<Data> onPreArt = subtract(reduceSummarizedObs(artInTheQ), reduceSummarizedObs(startedArtOnOrB4Q));
            List<Data> onPreArtAndCpt = intersection(cptThisQuarter, onPreArt);
            List<Data> onPreArtAndTb = intersection(reduceSummarizedObs(tbThisQuarter), onPreArt);
            List<Data> onPreArtAndDiagnosedTb = intersection(tbDiagnosedThisQuarter, onPreArt);
            List<Data> onPreArtAndStartedTb = intersection(startedTbThisQuarter, onPreArt);
            List<Data> onPreArtAssessedMalnutrition = intersection(assessed4Mal, onPreArt);
            List<Data> onPreArtMalnutrition = intersection(malnutrition, onPreArt);
            List<Data> onPreArtEligibleNotStarted = intersection(eligibleAndReady, onPreArt);
            List<Data> startedArtBasedOnCD4 = intersection(startedArtThisQ, artBasedOnCD4);

            String[] allFirst = {
                    "99005", "99040", "99015", "99016", "99041", "90002", "99039", "99143", "99042"
            };
            String[] allSecond = {
                    "99044", "99043", "99284", "99286", "99887", "99007", "99008", "99283", "99144", "99888"
            };
            String[] allFirstAndSecondChildren = {
                    "99006", "99885", "99884"
            };
            String[] allSecondAndFirstChildren = {
                    "99046"
            };
            String[] firstAndSecondChildren = {
                    "163017"
            };

            String[] third = {
                    "162987", "162986"
            };

            List<Data> allFirstLine = filterAndReduce(artRegimen, hasVal(allFirst));
            List<Data> firstLineAdult = filterData(filterAndReduce(artRegimen, hasVal(allFirstAndSecondChildren)), afterAge(12));
            List<Data> firstLineChildren1 = filterData(filterAndReduce(artRegimen, hasVal(allSecondAndFirstChildren)), beforeAge(13));
            List<Data> allSecondLine = filterAndReduce(artRegimen, hasVal(allSecond));
            List<Data> secondLineAdult = filterData(filterAndReduce(artRegimen, hasVal(allSecondAndFirstChildren)), afterAge(12));
            List<Data> secondLineChildren1 = filterData(filterAndReduce(artRegimen, hasVal(allFirstAndSecondChildren)), beforeAge(13));
            List<Data> thirdLine = filterAndReduce(artRegimen, hasVal(third));
            List<Data> hadArtRegimen = reduceSummarizedObs(artRegimen);

            Map<String, Long> enrolledB4Q = summarize(reduceSummarizedObs(summaryBe4Q), get106, zeros106);
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

            addData(intersection(reduceSummarizedObs(tbThisQuarter), hadArtRegimen), dataSet, "i", "24");
            addData(intersection(tbDiagnosedThisQuarter, hadArtRegimen), dataSet, "i", "25");
            addData(intersection(startedTbThisQuarter, hadArtRegimen), dataSet, "i", "26");
            addData(0, dataSet, "i", "27");
            addData(intersection(goodAdherence, hadArtRegimen), dataSet, "i", "28");
            addData(intersection(assessed4Mal, hadArtRegimen), dataSet, "i", "29");
            addData(intersection(malnutrition, hadArtRegimen), dataSet, "i", "30");

            List<SummarizedObs> all = joinSummarizedObs(dateObsBe4Q, deadPeople);

            Map<String, Object> data1 = get1061BCohorts(lastDate, allQuarters, all, connection);


            for (Map.Entry<String, Object> data : data1.entrySet()) {
                String label = data.getKey();
                dataSet.addData(new DataSetColumn(label, label, Object.class), data.getValue());
            }
        } catch (Exception e) {
            System.out.println("Some silly error is there  " + e.getMessage());
        }
        return dataSet;
    }
}