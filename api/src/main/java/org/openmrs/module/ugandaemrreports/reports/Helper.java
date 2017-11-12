package org.openmrs.module.ugandaemrreports.reports;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.openmrs.Cohort;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.reporting.data.patient.definition.SqlPatientDataDefinition;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.MapDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.reporting.evaluation.querybuilder.SqlQueryBuilder;
import org.openmrs.module.ugandaemrreports.common.*;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.sort;
import static java.util.Comparator.comparing;
import static org.openmrs.module.ugandaemrreports.reports.Predicates.*;

public class Helper {

    public static List<PersonDemographics> getPersonDemographics(Connection connection, String sql)
            throws SQLException {

        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        ResultSet rs = stmt.executeQuery(sql);
        List<PersonDemographics> personDemographics = new ArrayList<>();
        while (rs.next()) {
            PersonDemographics demographic = new PersonDemographics();
            demographic.setAddresses(rs.getString("addresses"));
            demographic.setAttributes(rs.getString("attributes"));
            demographic.setBirthDate(rs.getString("birthdate"));
            demographic.setGender(rs.getString("gender"));
            demographic.setIdentifiers(rs.getString("identifiers"));
            demographic.setNames(rs.getString("names"));
            demographic.setPersonId(rs.getInt("person_id"));
            personDemographics.add(demographic);
        }
        rs.close();
        stmt.close();

        return personDemographics;
    }


    public static String constructSQLInQuery(String column, String values) {
        return column + " IN(" + values + ")";
    }

    public static String joinQuery(String query1, String query2, Enums.UgandaEMRJoiner joiner) {
        return query1 + " " + joiner.toString() + " " + query2;
    }

    public static String joinQuery(Enums.UgandaEMRJoiner joiner, String... query) {
        return Joiner.on(" " + joiner.name() + " ").join(query);
    }


    public static String constructSQLQuery(String column, String rangeComparator, String value) {
        return column + " " + rangeComparator + " '" + value + "'";
    }


    public static Multimap<Integer, Date> getData(Connection connection, String sql, String columnLabel1, String columnLabel2) throws SQLException {

        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        Multimap<Integer, Date> result = TreeMultimap.create();
        while (rs.next()) {
            result.put(rs.getInt(columnLabel1), rs.getDate(columnLabel2));
        }
        rs.close();
        stmt.close();

        return result;
    }

    public static Map<Integer, Date> convert(Multimap<Integer, Date> m) {
        Map<Integer, Date> map = new HashMap<>();
        if (m == null) {
            return map;
        }

        for (Map.Entry<Integer, Collection<Date>> entry : m.asMap().entrySet()) {
            map.put(entry.getKey(), new ArrayList<>(entry.getValue()).get(0));
        }
        return map;
    }

    public static String convert(String concept) {
        Map<String, String> result = new HashMap<>();

        result.put("90012", "eMTCT");
        result.put("90016", "TB");
        result.put("99593", "YCC");
        result.put("90019", "Outreach");
        result.put("90013", "Out Patient");
        result.put("90015", "STI");
        result.put("90018", "Inpatient");
        result.put("90002", "Other");
        result.put("90079", "1");
        result.put("90073", "2");
        result.put("90078", "3");
        result.put("90071", "4");

        return result.get(concept);
    }

    public static String getOneData(Connection connection, String sql) throws SQLException {

        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String result = rs.getString(1);
        rs.close();
        stmt.close();
        return result;
    }

    public static ObsData getData(List<ObsData> data, String concept) {

        return data.stream()
                .filter(line -> line.getConceptId().compareTo(concept) == 0).findAny().orElse(null);
    }

    public static ObsData getData(List<ObsData> data, String yearMonth, String concept) {

        return data.stream()
                .filter(line -> line.getConceptId().compareTo(concept) == 0 && getObsPeriod(line.getEncounterDate(),
                        Enums.Period.MONTHLY).compareTo(yearMonth) == 0).findAny().orElse(null);
    }

    public static ObsData getData(List<ObsData> data, Integer yearMonth) {

        return data.stream()
                .filter(line -> getObsPeriod(line.getEncounterDate(),
                        Enums.Period.MONTHLY).compareTo(String.valueOf(yearMonth)) == 0).findAny().orElse(null);
    }

    public static List<ObsData> getData(Connection connection, String sql) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        List<ObsData> result = new ArrayList<>();
        while (rs.next()) {
            Integer patientId = rs.getInt(1);
            String conceptId = rs.getString(2);
            Integer encounterId = rs.getInt(3);
            Date encounterDate = rs.getDate(4);
            String val = rs.getString(5);
            String reportName = rs.getString(6);
            result.add(new ObsData(patientId, conceptId, encounterId, encounterDate, val, reportName));
        }
        rs.close();
        stmt.close();

        return result;
    }

    public static List<String> getData(Map<String, String> data, String yearQuarter, String concept) {
        String quarter = yearQuarter.substring(Math.max(yearQuarter.length() - 2, 0));
        String year = yearQuarter.substring(0, Math.min(yearQuarter.length(), 4));

        List<String> result = new ArrayList<>();

        ImmutableMap<String, List<String>> quarters =
                new ImmutableMap.Builder<String, List<String>>()
                        .put("Q1", Arrays.asList("01", "02", "03"))
                        .put("Q2", Arrays.asList("04", "05", "06"))
                        .put("Q3", Arrays.asList("07", "08", "09"))
                        .put("Q4", Arrays.asList("10", "11", "12"))
                        .build();

        List<String> periods = quarters.get(quarter);
        String r1 = data.get(year + periods.get(0) + concept);
        String r2 = data.get(year + periods.get(1) + concept);
        String r3 = data.get(year + periods.get(2) + concept);

        if (r1 != null) {
            result.add(r1);
        }
        if (r2 != null) {
            result.add(r2);
        }
        if (r3 != null) {
            result.add(r3);
        }
        return result;
    }

    public static String getData(Map<String, String> data, String concept) {

        Set<String> keys = data.keySet();

        String criteria = keys.stream()
                .filter(e -> e.endsWith(concept))
                .findAny().orElse(null);
        if (criteria != null) {
            return data.get(criteria);
        }

        return null;
    }


    public static Table<String, Integer, String> getDataTable(Connection connection, String sql) throws SQLException {

        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();
        Table<String, Integer, String> table = TreeBasedTable.create();
        while (rs.next()) {
            Integer patientId = rs.getInt(1);
            String conceptId = rs.getString(2);
            Date encounterDate = rs.getDate(3);
            String val = rs.getString(4);
            String encounterMonth = DateUtil.formatDate(encounterDate, "yyyyMM") + conceptId;
            table.put(encounterMonth, patientId, val);
        }
        rs.close();
        stmt.close();

        return table;
    }

    public static Map<String, Date> getClinicalStages(Map<String, String> data, String concept) {
        Map<String, Date> result = new HashMap<>();

        ImmutableMap<String, String> quarters =
                new ImmutableMap.Builder<String, String>()
                        .put("90033", "1")
                        .put("90034", "2")
                        .put("90035", "3")
                        .put("90036", "4")
                        .put("90293", "T1")
                        .put("90294", "T2")
                        .put("90295", "T3")
                        .put("90296", "T4")
                        .build();

        for (Map.Entry<String, String> dic : data.entrySet()) {
            String value = quarters.get(dic.getValue());
            String key = dic.getKey();
            if (key.endsWith(concept) && !result.containsValue(value)) {
                result.put(value, DateUtil.parseDate(key.substring(0, Math.min(key.length(), 6)), "yyyyMM"));
            }
        }
        return result;
    }

    public static String getMinimum(Map<String, String> data, String concept) {
        List<String> result = new ArrayList<>();

        for (Map.Entry<String, String> dic : data.entrySet()) {
            String key = dic.getKey();
            if (key.endsWith(concept)) {
                result.add(key.substring(0, Math.min(key.length(), 6)));
            }
        }
        sort(result);

        if (result.size() > 0) {

            String month = result.get(0).substring(Math.max(result.get(0).length() - 2, 0));
            String year = result.get(0).substring(0, Math.min(result.get(0).length(), 4));
            return month + "/" + year;
        }
        return "";
    }

    public static ObsData getFirstData(List<ObsData> data, String concept) {
        List<ObsData> filteredData = getDataAsList(data, concept);
        filteredData.sort(comparing(ObsData::getEncounterId));
        if (filteredData.size() > 0) {
            return filteredData.get(0);
        }
        return null;
    }

    public static List<ObsData> getDataAsList(List<ObsData> data, String concept) {
        return data.stream()
                .filter(line -> line.getConceptId().compareTo(concept) == 0)
                .collect(Collectors.toList());
    }

    public static String getObsPeriod(Date period, Enums.Period periodType) {
        LocalDate localDate = StubDate.dateOf(period);

        if (periodType == Enums.Period.YEARLY) {
            return localDate.toString("yyyy");
        } else if (periodType == Enums.Period.MONTHLY) {
            return localDate.toString("yyyyMM");
        } else if (periodType == Enums.Period.QUARTERLY) {
            return String.valueOf(localDate.getYear()) + "Q" + String.valueOf(((localDate.getMonthOfYear() - 1) / 3) + 1);
        } else {
            return localDate.getWeekyear() + "W" + localDate.weekOfWeekyear().get();
        }
    }

    public static String getObsPeriod2(Date period, Enums.Period periodType) {
        LocalDate localDate = StubDate.dateOf(period);

        if (periodType == Enums.Period.YEARLY) {
            return localDate.toString("yyyy");
        } else if (periodType == Enums.Period.MONTHLY) {
            return localDate.toString("yyyyM");
        } else if (periodType == Enums.Period.QUARTERLY) {
            return String.valueOf(localDate.getYear()) + String.valueOf(((localDate.getMonthOfYear() - 1) / 3) + 1);
        } else {
            return localDate.getWeekyear() + "W" + localDate.weekOfWeekyear().get();
        }
    }

    public static Integer getQuarter(List<LocalDate> localDates) {
        return getQuarter(localDates.get(0));

    }

    public static Integer getQuarter(LocalDate localDates) {
        return Integer.valueOf(String.valueOf(localDates.getYear()) + String.valueOf(((localDates.getMonthOfYear() - 1) / 3) + 1));

    }


    public static Map<String, String> artRegisterConcepts() {
        Map<String, String> concepts = new HashMap<>();
        concepts.put("functional status", "90235");
        concepts.put("inh dosage", "99604");
        concepts.put("tb start date", "90217");
        concepts.put("tb stop date", "90310");
        concepts.put("art start date", "99161");
        concepts.put("tb status", "90216");
        concepts.put("arv adh", "90221");
        concepts.put("cpt dosage", "99037");
        concepts.put("current regimen", "90315");
        concepts.put("return date", "5096");
        concepts.put("clinical stage", "90203");
        concepts.put("baseline weight", "99069");
        concepts.put("baseline cs", "99070");
        concepts.put("baseline cd4", "99071");
        concepts.put("baseline regimen", "99061");
        concepts.put("arv stop date", "99084");
        concepts.put("arv restart date", "99085");
        concepts.put("to date", "99165");
        concepts.put("ti date", "99160");
        concepts.put("entry", "90200");
        concepts.put("deaths", "0");
        concepts.put("weight", "90236");
        concepts.put("cd4", "5497");
        concepts.put("vl", "856");
        concepts.put("vl date", "163023");
        concepts.put("vl qualitative", "1305");
        concepts.put("arvdays", "99036");

        return concepts;
    }

    public static Map<String, String> preArtConcepts() {
        return new ImmutableMap.Builder<String, String>()
                .put("99161", "Art Start Date")
                .put("99037", "CPT Dosage")
                .put("99604", "INH Dosage")
                .put("99083", "Eligible for Art Clinical Stage")
                .put("99602", "Eligible for Art Pregnant")
                .put("99082", "Eligible for Art CD4")
                .put("99601", "Breast Feeding")
                .put("99600", "Tb for ART")
                .put("68", "Malnutrition")
                .put("5096", "Return visit date")
                .put("90200", "Entry point")
                .put("99115", "Other Entry point")
                .put("90203", "Who clinical stage")
                .put("90216", "TB Status")
                .put("90217", "TB Start Date")
                .put("90310", "TB stop date")
                .put("90297", "eligible date to start art")
                .put("90299", "eligible and ready date to start art")
                .put("99110", "TI")
                .put("99165", "To Date")
                .build();
    }

    public static Connection getDatabaseConnection(Properties props) throws ClassNotFoundException, SQLException {

        String driverClassName = props.getProperty("driver.class");
        String driverURL = props.getProperty("driver.url");
        String username = props.getProperty("user");
        String password = props.getProperty("password");
        Class.forName(driverClassName);
        return DriverManager.getConnection(driverURL, username, password);
    }

    public static ObsData viralLoad(List<ObsData> vls, Integer no) {

        if (vls != null && vls.size() > 0) {
            Map<Integer, List<ObsData>> vlsGroupedByEncounterId = vls.stream().collect(Collectors.groupingBy(ObsData::getEncounterId));

            List<Integer> keys = new ArrayList<>(vlsGroupedByEncounterId.keySet());

            sort(keys);

            Integer k = null;

            if (no == 6 && keys.size() > 0) {
                k = keys.get(0);
            } else if (no == 12 && keys.size() > 1) {
                k = keys.get(1);
            } else if (no == 24 && keys.size() > 2) {
                k = keys.get(2);
            } else if (no == 36 && keys.size() > 3) {
                k = keys.get(3);
            } else if (no == 48 && keys.size() > 4) {
                k = keys.get(4);
            } else if (no == 60 && keys.size() > 5) {
                k = keys.get(5);
            } else if (no == 72 && keys.size() > 6) {
                k = keys.get(6);
            }
            if (k != null) {
                if (vlsGroupedByEncounterId.get(k) != null && vlsGroupedByEncounterId.get(k).size() > 0) {
                    return vlsGroupedByEncounterId.get(k).get(0);
                }
            } else {
                return null;
            }
        }
        return null;
    }


    public static Map<Integer, List<PersonDemographics>> getPatientDemographics(Connection connection, String patients) throws SQLException {
        String where = "p.voided = 0";

        if (patients != null && StringUtils.isNotBlank(patients)) {
            where = joinQuery(where, constructSQLInQuery("p.person_id", patients), Enums.UgandaEMRJoiner.AND);
        }

        String q = "SELECT\n" +
                "\n" +
                "  person_id,\n" +
                "  gender,\n" +
                "  birthdate,\n" +
                "  (SELECT GROUP_CONCAT(CONCAT_WS(':', COALESCE(pit.uuid, ''), COALESCE(identifier, '')))\n" +
                "   FROM patient_identifier pi INNER JOIN patient_identifier_type pit\n" +
                "       ON (pi.identifier_type = pit.patient_identifier_type_id)\n" +
                "   WHERE pi.patient_id = p.person_id) AS 'identifiers',\n" +
                "  (SELECT GROUP_CONCAT(CONCAT_WS(':', COALESCE(pat.uuid, ''), COALESCE(value, '')))\n" +
                "   FROM person_attribute pa INNER JOIN person_attribute_type pat\n" +
                "       ON (pa.person_attribute_type_id = pat.person_attribute_type_id)\n" +
                "   WHERE p.person_id = pa.person_id)  AS 'attributes',\n" +
                "  (SELECT GROUP_CONCAT(CONCAT_WS(' ', COALESCE(given_name, ''), COALESCE(family_name, '')))\n" +
                "   FROM person_name pn\n" +
                "   WHERE p.person_id = pn.person_id)  AS 'names',\n" +
                "  (SELECT GROUP_CONCAT(CONCAT_WS(':', COALESCE(country, ''),COALESCE(county_district, ''), COALESCE(state_province, ''), COALESCE(address3, ''),\n" +
                "                                 COALESCE(address4, ''), COALESCE(address5, '')))\n" +
                "   FROM person_address pas\n" +
                "   WHERE p.person_id = pas.person_id) AS 'addresses'\n" +
                "FROM person p where " + where;

        List<PersonDemographics> personDemographics = getPersonDemographics(connection, q);
        return personDemographics.stream().collect(Collectors.groupingBy(PersonDemographics::getPersonId));

    }

    public static Map<String, String> processString(String value) {
        Map<String, String> result = new HashMap<>();

        List<String> splitData = Splitter.on(",").splitToList(value);

        for (String split : splitData) {
            List<String> keyValue = Splitter.on(":").splitToList(split);

            if (keyValue.size() == 2) {
                result.put(keyValue.get(0), keyValue.get(1));
            }
        }
        return result;
    }

    public static List<String> processString2(String value) {
        if (value != null) {
            List<String> splitData = Splitter.on(",").splitToList(value);
            if (splitData.size() > 0) {
                return Splitter.on(":").splitToList(splitData.get(0));
            }
        }
        return new ArrayList<>();
    }

    public static Connection sqlConnection() throws SQLException, ClassNotFoundException {

        Properties props = new Properties();
        props.setProperty("driver.class", "com.mysql.jdbc.Driver");
        props.setProperty("driver.url", Context.getRuntimeProperties().getProperty("connection.url"));
        props.setProperty("user", Context.getRuntimeProperties().getProperty("connection.username"));
        props.setProperty("password", Context.getRuntimeProperties().getProperty("connection.password"));
        return getDatabaseConnection(props);
    }

    public static Connection testSqlConnection() throws SQLException, ClassNotFoundException {
        Properties props = new Properties();
        props.setProperty("driver.class", "com.mysql.jdbc.Driver");
        props.setProperty("driver.url", "jdbc:mysql://localhost:3306/openmrs");
        props.setProperty("user", "openmrs");
        props.setProperty("password", "openmrs");
        return getDatabaseConnection(props);
    }

    public static Integer executeQuery(String query, Connection dbConn) throws SQLException {
        PreparedStatement stmt = dbConn.prepareStatement(query);
        return stmt.executeUpdate();
    }

    public static int createObsSummaryTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS obs_summary (\n" +
                "  encounter_type CHAR(20) NULL,\n" +
                "  concept        CHAR(10) NULL,\n" +
                "  y              INT(4)   NULL,\n" +
                "  m              INT(1)   NULL,\n" +
                "  q              INT(1)   NULL,\n" +
                "  ym             INT(6)   NULL,\n" +
                "  yq             INT(6)   NULL,\n" +
                "  vals           LONGTEXT NULL,\n" +
                "  grouped_by     INT(1) DEFAULT 1,\n" +
                "  total          INT(8)   NULL\n" +
                ")";

        return executeQuery(sql, connection);
    }

    public static List<SummarizedObs> getSummarizedObs(Connection connection, String conditions) throws SQLException {

        String sql = "SELECT encounter_type, concept, y, m, q, ym, yq, vals, grouped_by, total FROM obs_summary";
        if (StringUtils.isNotBlank(conditions)) {
            sql += " WHERE " + conditions;
        }
        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(sql);
        List<SummarizedObs> summarizedObs = new ArrayList<>();
        while (rs.next()) {
            SummarizedObs obs = new SummarizedObs();
            obs.setEncounterType(rs.getString(1));
            obs.setConcept(rs.getString(2));
            obs.setY(rs.getInt(3));
            obs.setM(rs.getInt(4));
            obs.setQ(rs.getInt(5));
            obs.setYm(rs.getInt(6));
            obs.setYq(rs.getInt(7));
            obs.setVals(rs.getString(8));
            obs.setGroupedBy(rs.getInt(9));
            obs.setTotal(rs.getInt(10));
            summarizedObs.add(obs);
        }
        rs.close();
        stmt.close();
        return summarizedObs;
    }

    public static void summarizeObs(Connection connection, String date) throws SQLException {
        createObsSummaryTable(connection);
        executeQuery("SET @@group_concat_max_len = 1000000;", connection);
        executeQuery(otherQuery(date), connection);
        executeQuery(valueDatetimeQuery(date), connection);
        executeQuery(encounterSummaryQuery(date), connection);
        executeQuery(deathQuery(date), connection);
    }

    public static Multimap<Integer, Integer> getFirstEncounters(Connection connection, String patients) throws SQLException {

        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        String query = "SELECT\n" +
                "  patient_id,\n" +
                "  encounter_type,\n" +
                "  min(encounter_id)\n" +
                "FROM encounter\n" +
                "WHERE voided = 0\n";
        if (StringUtils.isNotBlank(patients)) {
            query += String.format(" AND patient_id IN(%s)\n", patients);
        }
        query += "GROUP BY patient_id, encounter_type";
        ResultSet rs = stmt.executeQuery(query);
        Multimap<Integer, Integer> firstEncounters = ArrayListMultimap.create();
        while (rs.next()) {
            firstEncounters.put(rs.getInt(2), rs.getInt(3));
        }
        rs.close();
        stmt.close();
        return firstEncounters;
    }


    public static String encounterSummaryQuery(String date) {
        return "INSERT INTO obs_summary (encounter_type, concept, y, m, q, ym, yq, vals, grouped_by, total)\n" +
                "  SELECT\n" +
                "    IFNULL((SELECT et.encounter_type_id\n" +
                "            FROM encounter_type AS et\n" +
                "            WHERE et.encounter_type_id = e.encounter_type), 'ANY') AS encounter_type,\n" +
                "    'encounter'                                                    AS concept,\n" +
                "    YEAR(\n" +
                "        e.encounter_datetime)                                      AS y,\n" +
                "    MONTH(\n" +
                "        e.encounter_datetime)                                      AS m,\n" +
                "    QUARTER(\n" +
                "        e.encounter_datetime)                                      AS q,\n" +
                "    concat(YEAR(encounter_datetime), MONTH(\n" +
                "        encounter_datetime))                                       AS ym,\n" +
                "    concat(YEAR(encounter_datetime), QUARTER(\n" +
                "        encounter_datetime))                                       AS yq,\n" +
                "    group_concat(concat_ws(':', patient_id, encounter_id, (SELECT p.gender\n" +
                "                                                           FROM person p\n" +
                "                                                           WHERE p.person_id = e.patient_id),\n" +
                "                           (SELECT YEAR(e.encounter_datetime) - YEAR(birthdate)\n" +
                "                                   - (RIGHT(e.encounter_datetime, 5) <\n" +
                "                                      RIGHT(birthdate, 5))\n" +
                "                            FROM person p\n" +
                "                            WHERE p.person_id = e.patient_id), 'encounter', DATE(e.encounter_datetime),\n" +
                "                           voided))                                AS age_gender,\n" +
                "    3                                                              AS grouped_by,\n" +
                "    COUNT(DISTINCT\n" +
                "          patient_id)                                              AS total\n" +
                "  FROM encounter e\n" +
                String.format("  WHERE date_created > '%s'\n", date) +
                "  GROUP BY encounter_type, y, q, m;";
    }

    public static String otherQuery(String date) {
        return "INSERT INTO obs_summary (encounter_type, concept, y, m, q, ym, yq, vals, total)\n" +
                "  SELECT\n" +
                "    IFNULL((SELECT et.encounter_type_id\n" +
                "            FROM encounter_type AS et\n" +
                "            WHERE et.encounter_type_id =\n" +
                "                  (SELECT e.encounter_type\n" +
                "                   FROM encounter AS e\n" +
                "                   WHERE e.encounter_id = o.encounter_id)), 'ANY') AS encounter_type,\n" +
                "\n" +
                "    concept_id                                                     AS concept,\n" +
                "    YEAR(obs_datetime)                                             AS y,\n" +
                "    MONTH(obs_datetime)                                            AS m,\n" +
                "    QUARTER(obs_datetime)                                          AS q,\n" +
                "    concat(YEAR(obs_datetime), MONTH(obs_datetime))                AS ym,\n" +
                "    concat(YEAR(obs_datetime), QUARTER(obs_datetime))              AS yq,\n" +
                "    group_concat(concat_ws(':', person_id, encounter_id, (SELECT p.gender\n" +
                "                                                          FROM person p\n" +
                "                                                          WHERE p.person_id = o.person_id),\n" +
                "                           (SELECT YEAR(o.obs_datetime) - YEAR(birthdate)\n" +
                "                                   - (RIGHT(o.obs_datetime, 5) <\n" +
                "                                      RIGHT(birthdate, 5))\n" +
                "                            FROM person p\n" +
                "                            WHERE p.person_id = o.person_id), concept_id, value_numeric, value_coded, value_complex,\n" +
                "                           DATE(value_datetime),\n" +
                "                           value_drug, value_group_id,\n" +
                "                           value_modifier, value_text, voided))    AS age_gender,\n" +
                "    COUNT(DISTINCT person_id)                                      AS total\n" +
                "  FROM obs o\n" +
                String.format("  WHERE date_created > '%s'\n", date) +
                "  GROUP BY encounter_type, concept, y, q, m;";
    }


    public static String valueDatetimeQuery(String date) {
        return "INSERT INTO obs_summary (encounter_type, concept, y, m, q, ym, yq, vals, grouped_by, total)\n" +
                "  SELECT\n" +
                "    IFNULL((SELECT et.encounter_type_id\n" +
                "            FROM encounter_type AS et\n" +
                "            WHERE et.encounter_type_id =\n" +
                "                  (SELECT e.encounter_type\n" +
                "                   FROM encounter AS e\n" +
                "                   WHERE e.encounter_id = o.encounter_id)), 'ANY')                                       AS encounter_type,\n" +
                "    concept_id                                                                                           AS concept,\n" +
                "    YEAR(value_datetime)                                                                                 AS y,\n" +
                "    MONTH(value_datetime)                                                                                AS m,\n" +
                "    QUARTER(value_datetime)                                                                              AS q,\n" +
                "    concat(YEAR(value_datetime), MONTH(value_datetime))                                                  AS ym,\n" +
                "    concat(YEAR(value_datetime), QUARTER(value_datetime))                                                AS yq,\n" +
                "    group_concat(concat_ws(':', person_id, encounter_id, (SELECT p.gender\n" +
                "                                                          FROM person p\n" +
                "                                                          WHERE p.person_id = o.person_id),\n" +
                "                           (SELECT YEAR(o.value_datetime) - YEAR(birthdate)\n" +
                "                                   - (RIGHT(o.value_datetime, 5) <\n" +
                "                                      RIGHT(birthdate, 5))\n" +
                "                            FROM person p\n" +
                "                            WHERE p.person_id = o.person_id), concept_id, DATE(value_datetime), voided)) AS age_gender,\n" +
                "    2                                                                                                    AS grouped_by,\n" +
                "    COUNT(DISTINCT person_id)                                                                            AS total\n" +
                "  FROM obs o\n" +
                String.format("  WHERE value_datetime IS NOT NULL AND date_created > '%s'\n", date) +
                "  GROUP BY encounter_type, concept, y, q, m;";
    }

    public static String deathQuery(String date) {
        return "INSERT INTO obs_summary (encounter_type, concept, y, m, q, ym, yq, vals, grouped_by, total)\n" +
                "  SELECT\n" +
                "    'death'                                                           AS encounter_type,\n" +
                "    'death'                                                           AS concept,\n" +
                "    YEAR(p.death_date)                                               AS y,\n" +
                "    MONTH(p.death_date)                                              AS m,\n" +
                "    QUARTER(p.death_date)                                            AS q,\n" +
                "    concat(YEAR(p.death_date), MONTH(p.death_date))                  AS ym,\n" +
                "    concat(YEAR(p.death_date), QUARTER(p.death_date))                AS yq,\n" +
                "    group_concat(concat_ws(':', person_id, '4', p.gender,\n" +
                "                           YEAR(p.death_date) - YEAR(birthdate) - (RIGHT(p.death_date, 5) < RIGHT(birthdate, 5)),\n" +
                "                           'death', DATE(p.death_date), voided)) AS age_gender,\n" +
                "    4                                                                AS grouped_by,\n" +
                "    COUNT(DISTINCT person_id)                                        AS total\n" +
                "  FROM person p\n" +
                String.format("  WHERE p.death_date IS NOT NULL AND p.date_created > '%s'\n", date) +
                "  GROUP BY encounter_type, y, q, m;";
    }

    public static void addData(Map<String, Long> data, MapDataSet mapDataSet, String dataElement) {
        for (Map.Entry<String, Long> diss : data.entrySet()) {
            mapDataSet.addData(new DataSetColumn(dataElement + diss.getKey(), dataElement + diss.getKey(), Long.class), diss.getValue());
        }
    }

    public static void addData(Integer value, MapDataSet mapDataSet, String key, String dataElement) {
        mapDataSet.addData(new DataSetColumn(dataElement + key, dataElement + key, Integer.class), value);
    }

    public static void addData(List<Data> data, MapDataSet mapDataSet, String key, String dataElement) {
        mapDataSet.addData(new DataSetColumn(dataElement + key, dataElement + key, Integer.class), groupByPerson(data).keySet().size());
    }

    public static Function<Data, String> get106 = e -> {
        if (e.getGender().equals("M")) {
            if (e.getAge() <= 1) {
                return "a";
            } else if (e.getAge() >= 2 && e.getAge() <= 4) {
                return "c";
            } else if (e.getAge() >= 5 && e.getAge() <= 14) {
                return "e";
            } else if (e.getAge() >= 15) {
                return "g";
            }
        } else if (e.getGender().equals("F")) {
            if (e.getAge() <= 1) {
                return "b";
            } else if (e.getAge() >= 2 && e.getAge() <= 4) {
                return "d";
            } else if (e.getAge() >= 5 && e.getAge() <= 14) {
                return "f";
            } else if (e.getAge() >= 15) {
                return "h";
            }
        }
        return "-";
    };

    public static Function<Data, String> pregnant = e -> {
        if (e.getGender().equals("F")) {
            if (e.getAge() >= 5 && e.getAge() <= 14) {
                return "f";
            } else if (e.getAge() >= 15) {
                return "h";
            }
        }
        return "-";
    };

    public static Function<Data, String> pre = e -> {
        if (e.getAge() <= 14) {
            return "a";
        } else if (e.getAge() >= 15) {
            return "b";
        }
        return "-";
    };

    public static List<SummarizedObs> joinSummarizedObs(List<SummarizedObs>... summarizedObs) {
        List<SummarizedObs> results = new ArrayList<>();

        for (List<SummarizedObs> summarizedObs1 : summarizedObs) {
            results.addAll(summarizedObs1);
        }
        return results;
    }


    public static List<SummarizedObs> filter(List<SummarizedObs> summarizedObs, Predicate<SummarizedObs> predicate) {
        return summarizedObs.stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    public static List<Data> filterData(List<Data> summarizedObs, Predicate<Data> predicate) {
        return summarizedObs.stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    public static Map<String, Long> summarize(List<Data> data, Function<Data, String> ageGroups, List<String> zeros) {
        data = dataByEncounter(data, -1);

        Map<String, Long> initial = data.stream().collect(Collectors.groupingBy(ageGroups, Collectors.counting()));

        for (String zero : zeros) {
            initial.putIfAbsent(zero, 0L);
        }
        initial.put("i", (long) data.size());

        return initial;

    }

    public static Map<String, Long> summarize(List<Data> data, Function<Data, String> ageGroups, List<String> zeros, Predicate<Data> predicate) {
        data = dataByEncounter(data, -1);

        Map<String, Long> initial = summarize(data, ageGroups, zeros);

        for (String zero : zeros) {
            initial.putIfAbsent(zero, 0L);
        }

        List<Data> filtered = filterData(data, predicate);

        initial.put("i", (long) filtered.size());

        return initial;

    }

    public static List<Data> reduceSummarizedObs(Collection<SummarizedObs> encounterCodedObs) {
        List<Data> result = new ArrayList<>();

        for (SummarizedObs summarizedObs : encounterCodedObs) {
            result.addAll(summarizedObs.getData());
        }

        return result;

    }

    public static List<Data> reduceData(Map<Integer, List<Data>> data) {
        List<Data> result = new ArrayList<>();

        for (List<Data> d : data.values()) {
            result.addAll(d);
        }
        return result;
    }

    public static Set<Integer> reduceData(List<Data> data) {
        return data.stream()
                .map(Data::getPatientId).collect(Collectors.toSet());
    }

    public static Map<Integer, List<Data>> groupByPerson(List<Data> data) {
        return data.stream().collect(Collectors.groupingBy(Data::getPatientId));
    }

    public static List<Data> dataByEncounter(List<Data> data, Integer position) {
        List<Data> result = new ArrayList<>();

        for (Map.Entry<Integer, List<Data>> d : groupByPerson(data).entrySet()) {
            List<Data> vals = d.getValue();
            vals.sort(Comparator.comparing(Data::getEncounterId));
            if (vals.size() > 0) {
                if (position != -1) {
                    result.add(Iterables.getFirst(vals, null));
                } else {
                    result.add(Iterables.getLast(vals));
                }
            }
        }
        return result;
    }

    public static List<Data> combine(List<Data>... l1) {
        Collection data = new ArrayList<>();
        for (List<Data> l : l1) {
            data = CollectionUtils.union(data, l);
        }
        return new ArrayList<>(data);
    }

    public static List<Data> subtract(List<Data> l1, List<Data> l2) {

        Map<Integer, List<Data>> m1 = groupByPerson(l1);
        Map<Integer, List<Data>> m2 = groupByPerson(l2);

        Collection difference = CollectionUtils.subtract(m1.keySet(), m2.keySet());

        return reduceData(m1.entrySet().stream()
                .filter(map -> difference.contains(map.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static List<Data> intersection(List<Data> l1, List<Data> l2) {
        Map<Integer, List<Data>> m1 = groupByPerson(l1);
        Map<Integer, List<Data>> m2 = groupByPerson(l2);

        return reduceData(intersection(m1, m2));
    }

    public static Map<Integer, List<Data>> intersection(Map<Integer, List<Data>> m1, Map<Integer, List<Data>> m2) {

        Collection intersection = CollectionUtils.intersection(m1.keySet(), m2.keySet());

        return intersection(m1, intersection);
    }

    public static Map<Integer, List<Data>> intersection(Map<Integer, List<Data>> m1, Collection intersection) {

        return m1.entrySet().stream()
                .filter(map -> intersection.contains(map.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static List<Data> filterAndReduce(List<SummarizedObs> summarizedObs, Predicate<SummarizedObs> predicate) {
        return reduceSummarizedObs(filter(summarizedObs, predicate));
    }

    public static List<Data> filterAndReduce(List<SummarizedObs> summarizedObs, Predicate<SummarizedObs> predicate, Predicate<Data> dataPredicate) {
        return filterData(reduceSummarizedObs(filter(summarizedObs, predicate)), dataPredicate);
    }


    public static Map<String, Object> get106B(List<LocalDate> localDates, List<SummarizedObs> artStart, List<SummarizedObs> encounters, List<SummarizedObs> one06B, LocalDate end) {

        Integer quarter = getQuarter(localDates);
        Integer currentQuarter = getQuarter(end);

        DecimalFormat df = new DecimalFormat("###.##");

        Map<String, Object> data = new HashMap<>();

        Map<Integer, List<Data>> startedArt = groupByPerson(filterAndReduce(artStart, inTheQ(quarter)));
        Map<Integer, List<Data>> pregnant = intersection(startedArt, groupByPerson(filterAndReduce(one06B, hasConcepts("99072", "99603"), hasAnswers("90003"))));

        Map<Integer, List<Data>> baselineCD4 = groupByPerson(filterAndReduce(one06B, hasConcepts("99071"), afterAge(4)));
        Map<Integer, List<Data>> ti = groupByPerson(filterAndReduce(one06B, hasConcepts("99160", "90206")));
        Map<Integer, List<Data>> to = groupByPerson(filterAndReduce(one06B, hasConcepts("90306", "99165", "90211")));
        Map<Integer, List<Data>> stopped = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99084"), hasGroup(2))));
        Map<Integer, List<Data>> restarted = groupByPerson(filterAndReduce(one06B, and(hasConcepts("99085"), hasGroup(2))));
        Map<Integer, List<Data>> dead = groupByPerson(filterAndReduce(one06B, hasConcepts("death")));
        Map<Integer, List<Data>> cd4 = groupByPerson(filterAndReduce(one06B, and(hasConcepts("5497", "730"), onOrAfterQ(quarter), onOrBe4Q(currentQuarter))));
        Map<Integer, List<Data>> encountersAfter = groupByPerson(filterAndReduce(encounters, and(hasEncounterType("15"), onOrAfterQ(quarter), onOrBe4Q(currentQuarter))));


        Map<Integer, List<Data>> withBaseline = intersection(baselineCD4, startedArt);
        Map<Integer, List<Data>> transferIns = intersection(startedArt, ti);
        Map<Integer, List<Data>> transferOuts = intersection(startedArt, to);


        Map<Integer, List<Data>> withBaselinePregnant = intersection(baselineCD4, pregnant);
        Map<Integer, List<Data>> transferInsPregnant = intersection(pregnant, ti);
        Map<Integer, List<Data>> transferOutsPregnant = intersection(pregnant, to);


        Map<Integer, List<Data>> baseCD4 = groupByPerson(filterData(reduceData(withBaseline), max("500")));
        Map<Integer, List<Data>> baseCD4Pregnant = groupByPerson(filterData(reduceData(withBaselinePregnant), max("500")));


        Collection currentCohort = CollectionUtils.subtract(startedArt.keySet(), transferOuts.keySet());
        Collection currentCohortPregnant = CollectionUtils.subtract(pregnant.keySet(), transferOutsPregnant.keySet());


        Map<Integer, List<Data>> artStopped = intersection(stopped, currentCohort);
        Map<Integer, List<Data>> artRestarted = intersection(restarted, currentCohort);
        Map<Integer, List<Data>> artDead = intersection(dead, currentCohort);
        Map<Integer, List<Data>> artEncounters = intersection(encountersAfter, currentCohort);


        Map<Integer, List<Data>> artStoppedPregnant = intersection(stopped, currentCohortPregnant);
        Map<Integer, List<Data>> artRestartedPregnant = intersection(restarted, currentCohortPregnant);
        Map<Integer, List<Data>> artDeadPregnant = intersection(dead, currentCohortPregnant);
        Map<Integer, List<Data>> artEncountersPregnant = intersection(encountersAfter, currentCohortPregnant);


        double[] cd4Data = new double[baseCD4.size()];
        double[] cd4DataPregnant = new double[baseCD4Pregnant.size()];

        List<Integer> lost = new ArrayList<>();
        List<Integer> lost2Followup = new ArrayList<>();

        List<Integer> lostPregnant = new ArrayList<>();
        List<Integer> lost2FollowupPregnant = new ArrayList<>();

        int i = 0;
        int j = 0;

        int k = 0;
        int l = 0;

        for (Map.Entry<Integer, List<Data>> d : baseCD4.entrySet()) {
            Data dt = d.getValue().stream().max(comparing(Data::getEncounterId)).get();
            cd4Data[i++] = Double.valueOf(dt.getValue());
        }

        for (Map.Entry<Integer, List<Data>> d : baseCD4Pregnant.entrySet()) {
            Data dt = d.getValue().stream().max(comparing(Data::getEncounterId)).get();
            cd4DataPregnant[k++] = Double.valueOf(dt.getValue());
        }

        for (Map.Entry<Integer, List<Data>> e : artEncounters.entrySet()) {
            Data x = e.getValue().stream().max(Comparator.comparing(Data::getEncounterId)).get();
            LocalDate start = StubDate.dateOf(x.getValue());
            int days = Days.daysBetween(start, end).getDays();

            if (days >= 7 && days < 90) {
                lost.add(e.getKey());
            } else if (days >= 90) {
                lost2Followup.add(e.getKey());
            }

        }

        for (Map.Entry<Integer, List<Data>> e : artEncountersPregnant.entrySet()) {
            Data x = e.getValue().stream().max(Comparator.comparing(Data::getEncounterId)).get();
            LocalDate start = StubDate.dateOf(x.getValue());
            int days = Days.daysBetween(start, end).getDays();

            if (days >= 7 && days < 90) {
                lostPregnant.add(e.getKey());
            } else if (days >= 90) {
                lost2FollowupPregnant.add(e.getKey());
            }

        }

        Collection allStopped = CollectionUtils.subtract(artStopped.keySet(), artRestarted.keySet());
        Collection allStoppedPregnant = CollectionUtils.subtract(artStoppedPregnant.keySet(), artRestartedPregnant.keySet());


        Collection currentAlive = CollectionUtils.subtract(currentCohort, CollectionUtils.union(CollectionUtils.union(allStopped, artDead.keySet()), CollectionUtils.union(lost, lost2Followup)));
        Collection currentAlivePregnant = CollectionUtils.subtract(currentCohortPregnant, CollectionUtils.union(CollectionUtils.union(allStoppedPregnant, artDeadPregnant.keySet()), CollectionUtils.union(lostPregnant, lost2FollowupPregnant)));


        Map<Integer, List<Data>> artCD4 = intersection(cd4, currentAlive);
        Map<Integer, List<Data>> artCD4L500 = intersection(groupByPerson(filterData(reduceData(cd4), max("500"))), currentAlive);

        Map<Integer, List<Data>> artCD4Pregnant = intersection(cd4, currentAlivePregnant);
        Map<Integer, List<Data>> artCD4L500Pregnant = intersection(groupByPerson(filterData(reduceData(cd4), max("500"))), currentAlivePregnant);

        double[] cd4AfterData = new double[artCD4L500.size()];
        double[] cd4AfterDataPregnant = new double[artCD4L500Pregnant.size()];

        for (Map.Entry<Integer, List<Data>> d : artCD4L500.entrySet()) {
            Data dt = d.getValue().stream().max(comparing(Data::getEncounterId)).get();
            cd4AfterData[j++] = Double.valueOf(dt.getValue());
        }

        for (Map.Entry<Integer, List<Data>> d : artCD4L500Pregnant.entrySet()) {
            Data dt = d.getValue().stream().max(comparing(Data::getEncounterId)).get();
            cd4AfterDataPregnant[l++] = Double.valueOf(dt.getValue());
        }

        data.put("b2m", localDates.get(0).getMonthOfYear());
        data.put("b2y", Integer.valueOf(localDates.get(1).toString("yyyy")));

        data.put("b2mf", localDates.get(0).getMonthOfYear());
        data.put("b2yf", Integer.valueOf(localDates.get(1).toString("yyyy")));

        data.put("b3", CollectionUtils.subtract(startedArt.keySet(), transferIns.keySet()).size());
        data.put("b3f", CollectionUtils.subtract(pregnant.keySet(), transferInsPregnant.keySet()).size());

        if (withBaseline.size() > 0) {
            data.put("b4", df.format((double) baseCD4.size() / withBaseline.size()));

        } else {
            data.put("b4", "");
        }

        if (withBaselinePregnant.size() > 0) {
            data.put("b4f", df.format((double) baseCD4Pregnant.size() / withBaselinePregnant.size()));

        } else {
            data.put("b4f", "");
        }

        if (cd4Data.length > 0) {
            data.put("b5", StatUtils.percentile(cd4Data, 50));
        } else {
            data.put("b5", "");
        }

        if (cd4DataPregnant.length > 0) {
            data.put("b5f", StatUtils.percentile(cd4DataPregnant, 50));
        } else {
            data.put("b5f", "");
        }


        data.put("b6", transferIns.size());
        data.put("b6f", transferInsPregnant.size());

        data.put("b7", transferOuts.size());
        data.put("b7f", transferOutsPregnant.size());

        data.put("b8", currentCohort.size());
        data.put("b8f", currentCohortPregnant.size());

        data.put("b9", allStopped.size());
        data.put("b9f", allStoppedPregnant.size());

        data.put("b10", artDead.size());
        data.put("b10f", artDeadPregnant.size());

        data.put("b11", lost.size());
        data.put("b11f", lostPregnant.size());

        data.put("b12", lost2Followup.size());
        data.put("b12f", lost2FollowupPregnant.size());

        data.put("b13", currentAlive.size());
        data.put("b13f", currentAlivePregnant.size());

        if (currentCohort.size() > 0) {
            data.put("b14", df.format(currentAlive.size() * 100 / currentCohort.size()));
        } else {
            data.put("b14", "");
        }

        if (currentCohortPregnant.size() > 0) {
            data.put("b14f", df.format(currentAlivePregnant.size() * 100 / currentCohortPregnant.size()));

        } else {
            data.put("b14f", "");

        }

        if (artCD4.size() > 0) {
            data.put("b15", df.format((double) artCD4L500.size() / artCD4.size()));

        } else {
            data.put("b15", "");

        }

        if (artCD4Pregnant.size() > 0) {
            data.put("b15f", df.format((double) artCD4L500Pregnant.size() / artCD4Pregnant.size()));

        } else {
            data.put("b15f", "");

        }

        if (cd4AfterData.length > 0) {
            data.put("b16", StatUtils.percentile(cd4AfterData, 50));

        } else {
            data.put("b16", "");

        }

        if (cd4AfterDataPregnant.length > 0) {
            data.put("b16f", StatUtils.percentile(cd4AfterDataPregnant, 50));

        } else {
            data.put("b16f", "");

        }
        return data;
    }


    public static Map<String, Integer> getLostPatients(Connection connection, String cohort, String endDate) throws SQLException {
        String sql = "SELECT\n" +
                "  A.patient_id,\n" +
                "  CASE\n" +
                "  WHEN B.visit IS NULL\n" +
                "    THEN\n" +
                "      CASE\n" +
                "      WHEN DATEDIFF('2015-03-31', A.encounter) BETWEEN 8 AND 89\n" +
                "        THEN 'LOST'\n" +
                "      WHEN DATEDIFF('2015-03-31', A.encounter) >= 90\n" +
                "        THEN 'DROPPED'\n" +
                "      ELSE 'ACTIVE'\n" +
                "      END\n" +
                "  WHEN A.encounter >= B.visit AND B.visit IS NOT NULL\n" +
                "    THEN\n" +
                "      CASE\n" +
                "      WHEN DATEDIFF(A.encounter, B.visit) BETWEEN 8 AND 89\n" +
                "        THEN 'LOST'\n" +
                "      WHEN DATEDIFF(A.encounter, B.visit) >= 90\n" +
                "        THEN 'DROPPED'\n" +
                "      ELSE 'ACTIVE'\n" +
                "      END\n" +
                "  WHEN B.visit > A.encounter AND B.visit IS NOT NULL\n" +
                "    THEN\n" +
                "      CASE\n" +
                "      WHEN DATEDIFF('2015-03-31', B.visit) BETWEEN 8 AND 89\n" +
                "        THEN 'LOST'\n" +
                "      WHEN DATEDIFF('2015-03-31', B.visit) >= 90\n" +
                "        THEN 'DROPPED'\n" +
                "      ELSE 'ACTIVE'\n" +
                "      END\n" +
                "  END as status\n" +
                "FROM\n" +
                "  (SELECT\n" +
                "     patient_id,\n" +
                "     MAX(DATE(encounter_datetime)) AS encounter\n" +
                "   FROM encounter\n" +
                "   WHERE patient_id IN(1,2,3,4,5,6,7) AND voided = 0 AND encounter_datetime < '2015-03-31'\n" +
                "   GROUP BY patient_id) A\n" +
                "  LEFT JOIN\n" +
                "  (SELECT\n" +
                "     person_id,\n" +
                "     MAX(DATE(value_datetime)) AS visit\n" +
                "   FROM obs\n" +
                "   WHERE concept_id = 5096 AND voided = 0 AND person_id IN(1,2,3,4,5,6,7) AND value_datetime < '2015-03-31'\n" +
                "   GROUP BY person_id) B ON (A.patient_id = B.person_id)\n" +
                "  LEFT JOIN\n" +
                "  (SELECT\n" +
                "     person_id,\n" +
                "     MIN(DATE(value_datetime)) AS nextVisit\n" +
                "   FROM obs\n" +
                "   WHERE concept_id = 5096 AND voided = 0 AND person_id IN(1,2,3,4,5,6,7) AND value_datetime >= '2015-03-31') C\n" +
                "    ON (A.patient_id = C.person_id)\n" +
                "  LEFT JOIN\n" +
                "  (SELECT\n" +
                "     patient_id,\n" +
                "     MIN(DATE(encounter_datetime)) AS nextEncounter\n" +
                "   FROM encounter\n" +
                "   WHERE patient_id IN(1,2,3,4,5,6,7) AND voided = 0 AND encounter_datetime >= '2015-03-31'\n" +
                "   GROUP BY patient_id) D ON (A.patient_id = D.patient_id)";

        sql = sql.replace("2015-03-31", endDate).replace("1,2,3,4,5,6,7", cohort);

        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        ResultSet rs = stmt.executeQuery(sql);
        Set<Integer> lost = new HashSet<Integer>();
        Set<Integer> dropped = new HashSet<Integer>();
        Set<Integer> active = new HashSet<Integer>();
        while (rs.next()) {
            if (rs.getString(1).equals("LOST")) {
                lost.add(rs.getInt(2));
            } else if (rs.getString(1).equals("ACTIVE")) {
                active.add(rs.getInt(2));
            } else if (rs.getString(1).equals("DROPPED")) {
                dropped.add(rs.getInt(2));
            }

        }
        rs.close();
        stmt.close();

        return ImmutableMap.<String, Integer>builder().
                put("lost", lost.size()).
                put("dropped", dropped.size()).
                put("active", active.size()).
                build();
    }


    public static Map<Integer, Double> getPatientBaselineCD4Data(Connection connection, String cohort) throws SQLException {
        String sql = String.format("select o.person_id, o.value_numeric from obs o where o.voided = 0 and o.concept_id = 99071 and person_id in (select o.person_id from obs o inner join person p using(person_id) where o.concept_id = 99161 and o.voided = 0 and YEAR(o.value_datetime) - YEAR(p.birthdate) - (RIGHT(o.value_datetime, 5) < RIGHT(p.birthdate, 5)) > 5 and p.person_id in(%s))", cohort);

        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        ResultSet rs = stmt.executeQuery(sql);
        Map<Integer, Double> result = new HashMap<>();
        while (rs.next()) {
            result.put(rs.getInt(1), rs.getDouble(2));

        }
        rs.close();
        stmt.close();
        return result;
    }

    public static Map<Integer, Double> getPatientWithRecentCD4(Connection connection, String cohort, String endDate) throws SQLException {
        String sql = String.format("select DISTINCT A.person_id,A.value_numeric from (select o.person_id, o.value_numeric,o.obs_datetime from obs o where o.person_id in (%s) and o.concept_id = 5497 and obs_datetime <= '%s' and voided = 0) A  LEFT JOIN (select o.person_id, o.value_numeric,o.obs_datetime from obs o where o.person_id in (%s) and o.concept_id = 5497 and obs_datetime <= '%s' and voided = 0) B ON(A.person_id = B.person_id AND A.obs_datetime < B.obs_datetime) WHERE B.person_id IS NULL", cohort, endDate, cohort, endDate);
        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        ResultSet rs = stmt.executeQuery(sql);
        Map<Integer, Double> result = new HashMap<>();
        while (rs.next()) {
            result.put(rs.getInt(1), rs.getDouble(2));
        }
        rs.close();
        stmt.close();
        return result;
    }

    public static Map<Integer, Date> getPatientTransferredOut(Connection connection, String cohort, String endDate) throws SQLException {
        String sql = String.format("select o.person_id, o.value_datetime from obs o where o.voided = 0 and o.person_id in (%s) and o.concept_id = 99165 and o.value_datetime <= '%s'", cohort, endDate);
        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(sql);
        Map<Integer, Date> result = new HashMap<>();
        while (rs.next()) {
            result.put(rs.getInt(1), rs.getDate(2));

        }
        rs.close();
        stmt.close();
        return result;
    }

    public static List<Integer> getPregnantAtArtStart(Connection connection) throws SQLException {
        String sql = "select person_id from (select person_id,value_coded from obs where concept_id = 99072 and value_coded = 90003 and voided = 0 group by person_id union all select person_id,value_coded from obs where concept_id = 99603 and value_coded = 90003 and voided = 0 group by person_id) A group by person_id";
        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(sql);
        List<Integer> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getInt(1));
        }
        rs.close();
        stmt.close();
        return result;
    }

    public static Set<Integer> getPatientStopped(Connection connection, String cohort, String endDate) throws SQLException {
        String sqlStopped = String.format("select person_id, MAX(DATE(o.value_datetime)) from obs o where o.voided = 0 and o.person_id in (%s) and o.concept_id = 99084 and o.value_datetime <= '%s' group by person_id", cohort, endDate);
        String sqlRestarted = String.format("select person_id, MAX(DATE(o.value_datetime)) from obs o where o.voided = 0 and o.person_id in (%s) and o.concept_id = 99085 and o.value_datetime <= '%s' group by person_id", cohort, endDate);

        Statement stmtStopped = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmtStopped.setFetchSize(Integer.MIN_VALUE);

        ResultSet rsStopped = stmtStopped.executeQuery(sqlStopped);

        Map<Integer, Date> stopped = new HashMap<>();

        while (rsStopped.next()) {
            stopped.put(rsStopped.getInt(1), rsStopped.getDate(2));
        }
        rsStopped.close();
        stmtStopped.close();


        Statement stmtRestarted = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmtRestarted.setFetchSize(Integer.MIN_VALUE);

        ResultSet rsRestarted = stmtRestarted.executeQuery(sqlRestarted);

        Map<Integer, Date> reStarted = new HashMap<>();

        while (rsRestarted.next()) {
            reStarted.put(rsRestarted.getInt(1), rsRestarted.getDate(2));
        }
        rsRestarted.close();
        stmtRestarted.close();

        Set<Integer> clients = new HashSet<Integer>();

        for (Map.Entry<Integer, Date> o : stopped.entrySet()) {
            Date stopDate = o.getValue();
            Date restartDate = reStarted.get(o.getKey());
            if (restartDate != null) {
                if (restartDate.before(stopDate)) {
                    clients.add(o.getKey());
                }
            } else {
                clients.add(o.getKey());
            }
        }
        return clients;
    }

    public static Map<Integer, Date> getDeadPatients(Connection connection, String cohort, String endDate) throws SQLException {
        String sql = "select * from\n" +
                String.format("  (select person_id,Date(value_datetime) as death_date from obs where voided = 0 and concept_id = 90272 and person_id in(%s) and value_datetime < '%s'\n", cohort, endDate) +
                "union\n" +
                String.format("select person_id, DATE(death_date) from person WHERE death_date is not null and person_id in(%s) and death_date < '%s') A group by person_id", cohort, endDate);
        Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery(sql);
        Map<Integer, Date> result = new HashMap<>();
        while (rs.next()) {
            result.put(rs.getInt(1), rs.getDate(2));

        }
        rs.close();
        stmt.close();
        return result;
    }

}