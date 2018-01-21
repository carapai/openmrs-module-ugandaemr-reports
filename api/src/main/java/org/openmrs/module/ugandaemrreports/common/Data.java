package org.openmrs.module.ugandaemrreports.common;

import java.util.Date;

public class Data {
    private Integer patientId;
    private Integer obsId;
    private Integer encounterId;
    private String gender;
    private Date dob;
    private Integer age;
    private Integer concept;
    private String value;
    private Integer obsGroupId;
    private Integer voided;
    private Integer encounterType;
    private Date encounterDate;

    public Data() {
    }

    public Data(Integer patientId, Integer obsId, Integer encounterId, String gender, Date dob, Integer age, Integer concept, String value, Integer obsGroupId, Integer voided) {
        this.patientId = patientId;
        this.obsId = obsId;
        this.encounterId = encounterId;
        this.gender = gender;
        this.dob = dob;
        this.age = age;
        this.concept = concept;
        this.value = value;
        this.obsGroupId = obsGroupId;
        this.voided = voided;
    }

    public Integer getPatientId() {
        return patientId;
    }

    public void setPatientId(Integer patientId) {
        this.patientId = patientId;
    }

    public Integer getObsId() {
        return obsId;
    }

    public void setObsId(Integer obsId) {
        this.obsId = obsId;
    }

    public Integer getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(Integer encounterId) {
        this.encounterId = encounterId;
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

    public Integer getConcept() {
        return concept;
    }

    public void setConcept(Integer concept) {
        this.concept = concept;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getObsGroupId() {
        return obsGroupId;
    }

    public void setObsGroupId(Integer obsGroupId) {
        this.obsGroupId = obsGroupId;
    }

    public Integer getVoided() {
        return voided;
    }

    public void setVoided(Integer voided) {
        this.voided = voided;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }

    public Integer getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(Integer encounterType) {
        this.encounterType = encounterType;
    }

    public Date getEncounterDate() {
        return encounterDate;
    }

    public void setEncounterDate(Date encounterDate) {
        this.encounterDate = encounterDate;
    }
}