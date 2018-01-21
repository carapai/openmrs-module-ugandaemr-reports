package org.openmrs.module.ugandaemrreports.common;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.openmrs.module.reporting.common.DateUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Predicate;

import static org.openmrs.module.ugandaemrreports.reports.Helper.filterData;
import static org.openmrs.module.ugandaemrreports.reports.Helper.groupByPerson;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.hasPatient;


public class CohortTracker {
    private Map<String, Set<Integer>> patients = new HashMap<>();
    private Set<Integer> concepts = new HashSet<>();
    private Map<String, Predicate<Data>> indicators = new HashMap<>();
    private Map<String, Predicate<Data>> aggregations = new HashMap<>();
    private Connection connection;
    private Set<Integer> encounterTypes = new HashSet<>();
    private Date startDate;
    private Date endDate;

    public CohortTracker() {
    }

    public CohortTracker(Map<String, Set<Integer>> patients, Set<Integer> concepts, Set<Integer> encounterTypes, Date startDate, Date endDate) {
        this.patients = patients;
        this.concepts = concepts;
        this.encounterTypes = encounterTypes;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public Map<String, Set<Integer>> getPatients() {
        return patients;
    }

    public void setPatients(Map<String, Set<Integer>> patients) {
        this.patients = patients;
    }

    public Set<Integer> getConcepts() {
        return concepts;
    }

    public void setConcepts(Set<Integer> concepts) {
        this.concepts = concepts;
    }

    public Set<Integer> getEncounterTypes() {
        return encounterTypes;
    }

    public void setEncounterTypes(Set<Integer> encounterTypes) {
        this.encounterTypes = encounterTypes;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Map<String, Predicate<Data>> getIndicators() {
        return indicators;
    }

    public void setIndicators(Map<String, Predicate<Data>> indicators) {
        this.indicators = indicators;
    }

    public Map<String, Predicate<Data>> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Predicate<Data>> aggregations) {
        this.aggregations = aggregations;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public List<Data> getData() throws SQLException {
        List<Data> data = new ArrayList<>();
        String query = "SELECT\n" +
                "  e.patient_id,\n" +
                "  group_concat(\n" +
                "      (SELECT group_concat(\n" +
                "          concat_ws(':', e.encounter_type, o.concept_id, e.encounter_id, DATE(e.encounter_datetime),\n" +
                "                    CONCAT_WS(':', o.value_numeric, o.value_coded, DATE(o.value_datetime), o.value_text)))\n" +
                "       FROM obs o\n" +
                "       WHERE o.encounter_id = e.encounter_id AND o.voided = 0\n";
        if (concepts != null && concepts.size() > 0) {
            String conceptString = Joiner.on(",").join(getConcepts());
            query += String.format(" AND o.concept_id IN(%s))) AS obs ", conceptString);
        }
        query += "FROM encounter e WHERE voided = 0 ";
        if (encounterTypes != null && encounterTypes.size() > 0) {
            String encounterString = Joiner.on(",").join(getEncounterTypes());
            query += String.format(" AND e.encounter_type IN(%s) ", encounterString);
        }

        if (patients != null && patients.size() > 0) {
            List<String> st = new ArrayList<>();
            for (Set<Integer> ps : getPatients().values()) {
                st.add(Joiner.on(",").join(ps));
            }
            String patientString = Joiner.on(",").join(st);
            query += String.format(" AND e.patient_id IN(%s) ", patientString);
        } else {
            return data;
        }

        if (startDate != null && endDate != null) {
            String date1 = DateUtil.formatDate(startDate, "yyyy-MM-dd");
            String date2 = DateUtil.formatDate(endDate, "yyyy-MM-dd");
            query += String.format(" AND e.encounter_datetime BETWEEN '%s' AND '%s' ", date1, date2);
        } else if (startDate != null) {
            String date1 = DateUtil.formatDate(startDate, "yyyy-MM-dd");
            query += String.format(" AND e.encounter_datetime >= '%s' ", date1);
        } else if (endDate != null) {
            String date1 = DateUtil.formatDate(endDate, "yyyy-MM-dd");
            query += String.format(" AND e.encounter_datetime <= '%s' ", date1);
        }
        query += "GROUP BY e.patient_id;";

        Statement stmt = getConnection().createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {
            Integer patientId = rs.getInt(1);
            String encounters = rs.getString(2);
            if (StringUtils.isNotBlank(encounters)) {
                for (String ageGender : Splitter.on(",").splitToList(encounters)) {
                    List<String> splitter = Splitter.on(":").splitToList(ageGender);
                    if (splitter.size() == 5) {
                        Data d = new Data();
                        Integer encounterType = Integer.valueOf(splitter.get(0));
                        Integer concept = Integer.valueOf(splitter.get(1));
                        Integer encounterId = Integer.valueOf(splitter.get(2));
                        Date encounterDate = DateUtil.parseYmd(splitter.get(3));
                        String val = splitter.get(4);
                        d.setPatientId(patientId);
                        d.setEncounterType(encounterType);
                        d.setConcept(concept);
                        d.setEncounterDate(encounterDate);
                        d.setEncounterId(encounterId);
                        d.setValue(val);
                        data.add(d);
                    }
                }
            }
        }
        return data;
    }

    public Map<String, Map<Integer, List<Data>>> execute() throws SQLException {
        List<Data> data = getData();
        Map<String, Map<Integer, List<Data>>> result = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> cohort : patients.entrySet()) {
            List<Data> filtered = filterData(data, hasPatient(cohort.getValue()));
            result.put(cohort.getKey(), groupByPerson(filtered));
            Map<String, Set<Integer>> allAggregations = new HashMap<>();

            for (Map.Entry<String, Predicate<Data>> dis : aggregations.entrySet()) {
                Map<Integer, List<Data>> withAggregation = groupByPerson(filterData(filtered, dis.getValue()));
                result.put(cohort.getKey() + "." + dis.getKey(), withAggregation);
                allAggregations.put(dis.getKey(), withAggregation.keySet());
            }

            for (Map.Entry<String, Predicate<Data>> indicator : indicators.entrySet()) {
                List<Data> indicatorData = filterData(filtered, indicator.getValue());
                Map<Integer, List<Data>> withIndicator = groupByPerson(indicatorData);
                result.put(cohort.getKey() + "." + indicator.getKey(), withIndicator);

                for (Map.Entry<String, Set<Integer>> dis : allAggregations.entrySet()) {
                    Map<Integer, List<Data>> withAggregationAndIndicator = groupByPerson(filterData(indicatorData,
                            hasPatient(dis.getValue())));
                    result.put(cohort.getKey() + "." + indicator.getKey() + "." + dis.getKey(), withAggregationAndIndicator);
                }
            }
        }
        return result;
    }
}
