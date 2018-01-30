/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.ugandaemrreports.fragment.controller;

import org.apache.commons.lang3.StringUtils;
import org.openmrs.api.context.Context;
import org.openmrs.module.appui.UiSessionContext;
import org.openmrs.ui.framework.annotation.SpringBean;
import org.openmrs.ui.framework.fragment.FragmentModel;
import org.openmrs.ui.framework.page.PageModel;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.openmrs.module.ugandaemrreports.reports.Helper.*;

/**
 *  * Controller for a fragment that shows all users  
 */
public class HomeFragmentController {

    public void controller(UiSessionContext sessionContext, FragmentModel model) {
    }

    public void get(@SpringBean PageModel pageModel) throws Exception {
        try {
            Context.openSession();
            String lastSummarizeDate = getGlobalProperty("ugandaemrreports.lastSummarizeDate");
            Connection connection = sqlConnection();
            if (lastSummarizeDate != null && StringUtils.isNotBlank(lastSummarizeDate)) {
                summarizeObs(connection, lastSummarizeDate);
            } else {
                summarizeObs(connection, "1900-01-01");
            }

            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

            Date now = new Date();
            String newDate = dateFormat.format(now);
            setGlobalProperty("ugandaemrreports.lastSummarizeDate", newDate);

            Context.closeSession();
        } catch (Exception e) {
            pageModel.put("persons", "A problem occurred, please check your Internet connection");
            System.out.println("Error occured");
        }
    }

}
