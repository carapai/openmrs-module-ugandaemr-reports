package org.openmrs.module.ugandaemrreports.definition.data.evaluator;

import org.openmrs.Obs;
import org.openmrs.annotation.Handler;
import org.openmrs.module.reporting.data.patient.EvaluatedPatientData;
import org.openmrs.module.reporting.data.patient.definition.PatientDataDefinition;
import org.openmrs.module.reporting.data.patient.evaluator.PatientDataEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.reporting.evaluation.querybuilder.HqlQueryBuilder;
import org.openmrs.module.reporting.evaluation.service.EvaluationService;
import org.openmrs.module.ugandaemrreports.definition.data.definition.UgandaEMREncounterObsDataDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by carapai on 27/09/2017.
 */
@Handler(supports = UgandaEMREncounterObsDataDefinition.class, order = 50)
public class UgandaEMREncounterObsDataEvaluator implements PatientDataEvaluator {

    @Autowired
    private EvaluationService evaluationService;

    @Override
    public EvaluatedPatientData evaluate(PatientDataDefinition patientDataDefinition, EvaluationContext evaluationContext) throws EvaluationException {

        UgandaEMREncounterObsDataDefinition def = (UgandaEMREncounterObsDataDefinition) patientDataDefinition;
        EvaluatedPatientData c = new EvaluatedPatientData(def, evaluationContext);

        if (evaluationContext.getBaseCohort() != null && evaluationContext.getBaseCohort().isEmpty()) {
            return c;
        }

        HqlQueryBuilder q = new HqlQueryBuilder();
        q.select("o.personId", "o");
        q.from(Obs.class, "o");
        q.whereEqual("o.voided", false);

        if (def.getEncounterType() != null) {
            q.whereEqual("o.encounter.encounterType", def.getEncounterType());
        }

        if (def.getStartDate() != null && def.getEndDate() != null) {
            q.whereBetweenInclusive("o.encounter.encounterDatetime", def.getStartDate(), def.getEndDate());
        } else if (def.getStartDate() != null) {
            q.whereGreaterOrEqualTo("o.encounter.encounterDatetime", def.getStartDate());
        } else if (def.getEndDate() != null) {
            q.whereLessOrEqualTo("o.encounter.encounterDatetime", def.getStartDate());
        }

        if (def.getEncounters() != null && def.getEncounters().size() > 0) {
            q.whereIn("o.encounter", def.getEncounters());
        }

        if (def.getConcepts() != null && def.getConcepts().size() > 0) {
            q.whereIn("o.concept", def.getConcepts());
        }
        if (def.getCohort() != null && def.getCohort().size() > 0) {
            q.whereIn("o.person", def.getCohort());
        }
        q.orderAsc("o.encounter.encounterDatetime");

        List<Object[]> results = evaluationService.evaluateToList(q, evaluationContext);
        for (Object[] row : results) {
            Integer pId = (Integer) row[0];
            Obs o = (Obs) row[1];
            List<Obs> obsForPatient = (List<Obs>) c.getData().get(pId);
            if (obsForPatient == null) {
                obsForPatient = new ArrayList<>();
                c.getData().put(pId, obsForPatient);
            }
            obsForPatient.add(o);
        }
        return c;
    }
}
