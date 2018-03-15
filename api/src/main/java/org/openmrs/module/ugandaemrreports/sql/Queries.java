package org.openmrs.module.ugandaemrreports.sql;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.openmrs.module.ugandaemrreports.reports.Helper.getDatabaseConnection;
import static org.openmrs.module.ugandaemrreports.sql.Constants.*;

public class Queries {

    private static Connection connection;

    private static final String OBS_QUERY = "SELECT\n" +
            String.format("  %s AS obs_id,\n", OBS_ID) +
            String.format("  %s AS person_id,\n", OBS_PERSON) +
            String.format("  IFNULL(%s, 0) AS encounter_type,\n", OBS_ENCOUNTER_TYPE) +
            String.format("  IFNULL(%s, 0) AS encounter_id,\n", OBS_ENCOUNTER) +
            String.format("  IFNULL(%s, '0000-00-00') AS encounter_date,\n", OBS_ENCOUNTER_DATE) +
            // String.format("  IF(encounter_id IN %s, 1, 0) AS is_first_encounter,\n", OBS_FIRST_ENCOUNTER) +
            String.format("  %s AS concept,\n", OBS_CONCEPT) +
            String.format("  IFNULL(%s,0) AS obs_group,\n", OBS_GROUP) +
            String.format("  %s AS y,\n", OBS_YEAR) +
            String.format("  %s AS m,\n", OBS_MONTH) +
            String.format("  %s AS q,\n", OBS_QUARTER) +
            String.format("  %s AS obs_date,\n", OBS_DATE) +
            //String.format("  %s AS location,\n", OBS_LOCATION) +
            // String.format("  IF(obs_id IN %s, 1, 0) AS is_first_obs,\n", OBS_FIRST) +
            // String.format("  %s AS gender,\n", OBS_GENDER) +
            // String.format("  %s AS birth_date,\n", OBS_BIRTH_DATE) +
            // String.format("  %s AS age,\n", OBS_AGE) +
            String.format("  %s AS val\n", OBS_VALUE) +
            "FROM obs o\n";


    static {
        try {
            connection = testSqlConnection();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static ResultSet obs() throws SQLException {
        Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        return stmt.executeQuery(OBS_QUERY);
    }

    public static ResultSet obs(String where) throws SQLException {

        Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        return stmt.executeQuery(String.format(OBS_QUERY + " WHERE %s", where));
    }

    public static List<Result> obs(String conditions, List<String> columns) throws SQLException {

        ResultSet rs;

        if (StringUtils.isNotBlank(conditions)) {
            rs = obs(conditions);
        } else {
            rs = obs();
        }
        List<Result> results = new ArrayList<>();
        while (rs.next()) {
            Result obs = new Result();
            for (int i = 0; i < columns.size(); i++) {
                obs.set(columns.get(i), rs.getString(i + 1));
            }
            results.add(obs);
        }
        rs.close();
        return results;
    }

    public static Q query(String column, String value) {
        return new Q(column + " = '" + value + "'");
    }

    public static Q l(String column, String value) {
        return new Q(column + " < '" + value + "'");
    }

    public static Q le(String column, String value) {
        return new Q(column + " <= '" + value + "'");
    }

    public static Q q(String column, String value) {
        return new Q(column + " > '" + value + "'");
    }

    public static Q qe(String column, String value) {
        return new Q(column + " >= '" + value + "'");
    }

    public static Q n(String column) {
        return new Q(column + " IS NULL");
    }

    public static Q b(String column, String val1, String val2) {
        return new Q(column + " BETWEEN '" + val1 + "' AND '" + val2 + "'");
    }

    public static Q in(String column, Collection<String> data) {
        String s = Joiner.on(",").join(data.stream().map(e -> "'" + e + "'").collect(Collectors.toList()));
        return new Q(column + " IN (" + s + ")");
    }

    public static Q in(String column, String... data) {
        String s = Joiner.on(",").join(Arrays.stream(data).map(e -> "'" + e + "'").collect(Collectors.toList()));
        return new Q(column + " IN (" + s + ")");
    }

    public static Q join(String joiner, Q... q) {
        return new Q(Joiner.on(joiner).join(q));
    }

    public static Q join(String joiner, Collection<Q> q) {
        return new Q(Joiner.on(joiner).join(q));
    }


    public static Connection testSqlConnection() throws SQLException, ClassNotFoundException {
        Properties props = new Properties();
        props.setProperty("driver.class", "com.mysql.jdbc.Driver");
        props.setProperty("driver.url", "jdbc:mysql://localhost:3306/ugandaemr");
        props.setProperty("user", "openmrs");
        props.setProperty("password", "openmrs");
        return getDatabaseConnection(props);
    }
}
