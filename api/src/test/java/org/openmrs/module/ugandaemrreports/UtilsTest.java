package org.openmrs.module.ugandaemrreports;

import org.junit.Test;
import org.openmrs.module.ugandaemrreports.sql.Q;
import org.openmrs.module.ugandaemrreports.sql.Queries;
import org.openmrs.module.ugandaemrreports.sql.Result;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.openmrs.module.ugandaemrreports.sql.Constants.*;

public class UtilsTest {


    @Test
    public void testQ() throws SQLException {

        List<String> concepts = Arrays.asList("99037", "99033", "90236", "5090", "99082", "99160", "90217", "99110",
                "90315", "99072", "99603", "90041", "90012", "90200", "90221", "90216", "99030", "68", "460");
        List<String> concepts2 = Arrays.asList("99604", "99604", "99161", "90299");
        List<String> columns = Arrays.asList("obs", "patient", "encounter_type", "encounter", "encounter_date", "concept", "obs_group", "y", "m", "q", "obs_date", "val");

        Q encounterObs = Queries.in(OBS_CONCEPT, concepts).e(OBS_QUARTER, "20171").bracket();
        Q summaryObs = Queries.in(OBS_CONCEPT, concepts2).e(OBS_ENCOUNTER_TYPE, "8").e(OBS_QUARTER, "20171").bracket();

        List<Result> results = Queries.join("OR", encounterObs, summaryObs).obs(columns);
    }
}
