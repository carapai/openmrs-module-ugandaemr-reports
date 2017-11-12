package org.openmrs.module.ugandaemrreports.definition.data.definition;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.EncounterType;
import org.openmrs.Person;
import org.openmrs.module.reporting.data.BaseDataDefinition;
import org.openmrs.module.reporting.data.patient.definition.PatientDataDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by carapai on 27/09/2017.
 */
public class UgandaEMREncounterObsDataDefinition extends BaseDataDefinition implements PatientDataDefinition {

    //****** CONSTRUCTORS ******

    /**
     * Default Constructor
     */
    public UgandaEMREncounterObsDataDefinition() {
        super();
    }

    @ConfigurationProperty
    private Date startDate;

    @ConfigurationProperty
    private Date endDate;

    @ConfigurationProperty
    private EncounterType encounterType;

    @ConfigurationProperty
    private List<Concept> concepts;

    @ConfigurationProperty
    private Collection<Person> cohort;

    @ConfigurationProperty
    private Collection<Encounter> encounters;

    //***** INSTANCE METHODS *****

    /**
     * @see org.openmrs.module.reporting.data.DataDefinition#getDataType()
     */
    public Class<?> getDataType() {
        return List.class;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public EncounterType getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(EncounterType encounterType) {
        this.encounterType = encounterType;
    }

    public List<Concept> getConcepts() {
        return concepts;
    }

    public void setConcepts(List<Concept> concepts) {
        this.concepts = concepts;
    }

    public Collection<Person> getCohort() {
        return cohort;
    }

    public void setCohort(Collection<Person> cohort) {
        this.cohort = cohort;
    }

    public Collection<Encounter> getEncounters() {
        return encounters;
    }

    public void setEncounters(Collection<Encounter> encounters) {
        this.encounters = encounters;
    }
}
