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

    private String encounterType;
    private String concept;
    private Integer y;
    private Integer q;
    private Integer m;
    private Integer ym;
    private Integer yq;
    private String vals;
    private Integer voided;
    private Integer groupedBy;
    private Integer total;

    public SummarizedObs() {
    }

    public SummarizedObs(String encounterType, String concept, Integer y, Integer m, Integer q, Integer ym, Integer yq, String vals, Integer voided, Integer groupedBy, Integer total) {
        this.encounterType = encounterType;
        this.concept = concept;
        this.y = y;
        this.q = q;
        this.m = m;
        this.ym = ym;
        this.yq = yq;
        this.vals = vals;
        this.voided = voided;
        this.groupedBy = groupedBy;
        this.total = total;
    }


    public String getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(String encounterType) {
        this.encounterType = encounterType;
    }

    public String getConcept() {
        return concept;
    }

    public void setConcept(String concept) {
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

    public String getVals() {
        return vals;
    }

    public void setVals(String vals) {
        this.vals = vals;
    }

    public Integer getVoided() {
        return voided;
    }

    public void setVoided(Integer voided) {
        this.voided = voided;
    }

    public Integer getGroupedBy() {
        return groupedBy;
    }

    public void setGroupedBy(Integer groupedBy) {
        this.groupedBy = groupedBy;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Data> getData() {
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


}