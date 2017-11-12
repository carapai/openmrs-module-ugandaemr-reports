package org.openmrs.module.ugandaemrreports.common;

public class Data {
    private Integer patientId;
    private Integer encounterId;
    private String gender;
    private Integer age;
    private String concept;
    private String value;
    private Integer voided;

    public Data(Integer patientId, Integer encounterId, String gender, Integer age, String concept, String value, Integer voided) {
        this.patientId = patientId;
        this.encounterId = encounterId;
        this.gender = gender;
        this.age = age;
        this.concept = concept;
        this.value = value;
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

    public String getConcept() {
        return concept;
    }

    public void setConcept(String concept) {
        this.concept = concept;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getVoided() {
        return voided;
    }

    public void setVoided(Integer voided) {
        this.voided = voided;
    }
}