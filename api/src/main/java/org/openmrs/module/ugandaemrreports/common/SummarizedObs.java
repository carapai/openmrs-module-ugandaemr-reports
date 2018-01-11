package org.openmrs.module.ugandaemrreports.common;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import org.openmrs.module.ugandaemrreports.reports.*;
import org.openmrs.module.ugandaemrreports.reports.Helper;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by carapai on 16/07/2017.
 */
public class SummarizedObs {

    private Integer encounterType;
    private Integer concept;
    private String val;
    private Integer y;
    private Integer q;
    private Integer m;
    private Integer ym;
    private Integer yq;
    private List<Data> ageGender;
    private Integer total;

    public SummarizedObs() {
    }

    public SummarizedObs(Integer encounterType, Integer concept, String val, Integer y, Integer m, Integer q, Integer ym, Integer yq, List<Data> ageGender, Integer total) {
        this.encounterType = encounterType;
        this.concept = concept;
        this.val = val;
        this.y = y;
        this.q = q;
        this.m = m;
        this.ym = ym;
        this.yq = yq;
        this.ageGender = ageGender;
        this.total = total;
    }


    public Integer getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(Integer encounterType) {
        this.encounterType = encounterType;
    }

    public Integer getConcept() {
        return concept;
    }

    public void setConcept(Integer concept) {
        this.concept = concept;
    }

    public Integer getY() {
        return y;
    }

    public void setY(Integer y) {
        this.y = y;
    }

    public Integer getQ() {
        return q;
    }

    public void setQ(Integer q) {
        this.q = q;
    }

    public Integer getM() {
        return m;
    }

    public void setM(Integer m) {
        this.m = m;
    }

    public Integer getYm() {
        return ym;
    }

    public void setYm(Integer ym) {
        this.ym = ym;
    }

    public Integer getYq() {
        return yq;
    }

    public void setYq(Integer yq) {
        this.yq = yq;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public List<Data> getAgeGender() {
        return ageGender;
    }

    public void setAgeGender(List<Data> ageGender) {
        this.ageGender = ageGender;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    /*public List<Data> getData() {
        List<Data> data = new ArrayList<>();
        for (String ageGender : Splitter.on(",").splitToList(getVals())) {
            List<String> splitter = Splitter.on(":").splitToList(ageGender);
            if (splitter.size() == 7) {
                Integer patientId = Integer.valueOf(splitter.get(0));
                Integer encounterId = Integer.valueOf(splitter.get(1));
                String gender = splitter.get(2);
                Integer age = Integer.valueOf(splitter.get(3));
                String concept = splitter.get(4);
                String value = splitter.get(5);
                Integer voided = Integer.valueOf(splitter.get(6));
                if (voided == 0) {
                    data.add(new Data(patientId, encounterId, gender, age, concept, value, voided));
                }
            }
        }
        return data;
    }
    */

}