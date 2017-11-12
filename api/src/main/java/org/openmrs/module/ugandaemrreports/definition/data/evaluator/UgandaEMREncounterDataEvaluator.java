package org.openmrs.module.ugandaemrreports.definition.data.evaluator;

import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.annotation.Handler;
import org.openmrs.module.reporting.data.patient.EvaluatedPatientData;
import org.openmrs.module.reporting.data.patient.definition.PatientDataDefinition;
import org.openmrs.module.reporting.data.patient.evaluator.PatientDataEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.reporting.evaluation.querybuilder.HqlQueryBuilder;
import org.openmrs.module.reporting.evaluation.service.EvaluationService;
import org.openmrs.module.ugandaemrreports.definition.data.definition.UgandaEMREncounterDataDefinition;
import org.openmrs.module.ugandaemrreports.definition.data.definition.UgandaEMREncounterObsDataDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by carapai on 27/09/2017.
 */
@Handler(supports = UgandaEMREncounterDataDefinition.class, order = 50)
public class UgandaEMREncounterDataEvaluator implements PatientDataEvaluator {

    @Autowired
    private EvaluationService evaluationService;

    @Override
    public EvaluatedPatientData evaluate(PatientDataDefinition patientDataDefinition, EvaluationContext evaluationContext) throws EvaluationException {

        UgandaEMREncounterDataDefinition def = (UgandaEMREncounterDataDefinition) patientDataDefinition;
        EvaluatedPatientData c = new EvaluatedPatientData(def, evaluationContext);

        if (evaluationContext.getBaseCohort() != null && evaluationContext.getBaseCohort().isEmpty()) {
            return c;
        }

        HqlQueryBuilder q = new HqlQueryBuilder();
        q.select("e.patient.patientId", "e");
        q.from(Encounter.class, "e");
        q.whereEqual("e.voided", false);

        if (def.getEncounterType() != null) {
            q.whereEqual("e.encounterType", def.getEncounterType());
        }

        if (def.getStartDate() != null && def.getEndDate() != null) {
            q.whereBetweenInclusive("e.encounterDatetime", def.getStartDate(), def.getEndDate());
        } else if (def.getStartDate() != null) {
            q.whereGreaterOrEqualTo("e.encounterDatetime", def.getStartDate());
        } else if (def.getEndDate() != null) {
            q.whereLessOrEqualTo("e.encounterDatetime", def.getStartDate());
        }

        if (def.getPatients() != null && def.getPatients().size() > 0) {
            q.whereIn("e.patient", def.getPatients());
        }

        List<Object[]> results = evaluationService.evaluateToList(q, evaluationContext);
        for (Object[] row : results) {
            Integer pId = (Integer) row[0];
            Encounter e = (Encounter) row[1];
            List<Encounter> encounters = (List<Encounter>) c.getData().get(pId);
            if (encounters == null) {
                encounters = new ArrayList<>();
                c.getData().put(pId, encounters);
            }
            encounters.add(e);
        }
        return c;
    }
}
