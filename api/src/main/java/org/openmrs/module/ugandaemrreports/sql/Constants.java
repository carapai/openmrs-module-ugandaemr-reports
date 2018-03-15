package org.openmrs.module.ugandaemrreports.sql;

public class Constants {
    public static final String OBS_ID = "o.obs_id";
    public static final String OBS_PERSON = "o.person_id";
    public static final String OBS_ENCOUNTER = "o.encounter_id";
    public static final String OBS_ENCOUNTER_TYPE = "(SELECT et.encounter_type_id FROM encounter_type AS et " +
            "WHERE et.encounter_type_id = (SELECT e.encounter_type FROM encounter AS e WHERE e.encounter_id = " +
            "o.encounter_id))";
    public static final String OBS_ENCOUNTER_DATE = "(SELECT DATE(et.encounter_datetime) FROM encounter et " +
            "WHERE et.encounter_id = o.encounter_id)";
    public static final String OBS_FIRST_ENCOUNTER = "(SELECT t1.encounter_id FROM encounter t1 WHERE t1.encounter_id = " +
            "(SELECT t2.encounter_id FROM encounter t2 WHERE t2.patient_id = t1.patient_id AND t2.encounter_type = " +
            "t1.encounter_type AND t2.voided = 0 ORDER BY t2.encounter_datetime ASC LIMIT 1))";
    public static final String OBS_CONCEPT = "o.concept_id";
    public static final String OBS_YEAR = "YEAR(ifnull(o.value_datetime, o.obs_datetime))";
    public static final String OBS_MONTH = "concat(YEAR(ifnull(o.value_datetime, o.obs_datetime)), " +
            "MONTH(ifnull(o.value_datetime, o.obs_datetime)))";
    public static final String OBS_QUARTER = "concat(YEAR(ifnull(o.value_datetime, o.obs_datetime)), " +
            "QUARTER(ifnull(o.value_datetime, o.obs_datetime)))";
    public static final String OBS_DATE = "DATE(o.obs_datetime)";
    public static final String OBS_LOCATION = "o.location_id";
    public static final String OBS_FIRST = "(SELECT o1.obs_id FROM obs o1 WHERE o1.obs_id = " +
            "(SELECT o2.obs_id FROM obs o2 WHERE o2.person_id = o1.person_id AND o2.concept_id = o1.concept_id AND " +
            "o2.voided = 0 ORDER BY o2.obs_datetime ASC LIMIT 1))";
    public static final String OBS_GENDER = "(SELECT p.gender FROM person p WHERE p.person_id = o.person_id)";
    public static final String OBS_BIRTH_DATE = "(SELECT p.birthdate FROM person p WHERE p.person_id = o.person_id)";
    public static final String OBS_AGE = "(SELECT YEAR(ifnull(o.value_datetime, o.obs_datetime)) - YEAR(p.birthdate) - " +
            "(RIGHT(ifnull(o.value_datetime, o.obs_datetime), 5) < RIGHT(p.birthdate, 5)) " +
            "FROM person p WHERE p.person_id = o.person_id)";
    public static final String OBS_VALUE = "concat_ws(':', o.value_coded, o.value_numeric, o.value_text, " +
            "DATE(o.value_datetime), o.value_coded_name_id, o.value_complex, o.value_drug, o.value_modifier, " +
            "o.value_group_id)";

    public static final String OBS_GROUP = "o.obs_group_id";

}
