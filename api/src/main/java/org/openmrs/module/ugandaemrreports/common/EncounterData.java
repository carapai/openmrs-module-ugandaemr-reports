package org.openmrs.module.ugandaemrreports.common;

import java.util.Date;

public class EncounterData {
    private Integer patientId;
    private Integer encounterId;
    private String gender;
    private Integer age;
    private Date encounterDate;

    private Integer voided;

    public EncounterData() {
    }

    public EncounterData(Integer patientId, Integer encounterId, String gender, Integer age, Date encounterDate, Integer voided) {
        this.patientId = patientId;
        this.encounterId = encounterId;
        this.gender = gender;
        this.age = age;
        this.encounterDate = encounterDate;
        this.voided = voided;
    }

    public Integer getPatientId() {
        return patientId;
    }

    public void setPatientId(Integer patientId) {
        this.patientId = patientId;
    }

    public Integer getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(Integer encounterId) {
        this.encounterId = encounterId;
    }

    public Date getEncounterDate() {
        return encounterDate;
    }

    public void setEncounterDate(Date encounterDate) {
        this.encounterDate = encounterDate;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getVoided() {
        return voided;
    }

    public void setVoided(Integer voided) {
        this.voided = voided;
    }
}