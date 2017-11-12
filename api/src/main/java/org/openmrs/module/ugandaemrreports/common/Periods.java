package org.openmrs.module.ugandaemrreports.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.openmrs.module.reporting.common.DateUtil;

import java.util.*;

/**
 */
public class Periods {

    public static LocalDate quarterStartFor(LocalDate date) {
        return date.withDayOfMonth(1).withMonthOfYear((((date.getMonthOfYear() - 1) / 3) * 3) + 1);
    }

    public static LocalDate monthStartFor(LocalDate date) {
        return date.withDayOfMonth(1);
    }

    public static LocalDate monthEndFor(LocalDate date) {
        return date.plusMonths(1).withDayOfMonth(1).minusDays(1);
    }

    public static LocalDate quarterEndFor(LocalDate date) {
        return quarterStartFor(date).plusMonths(3).minusDays(1);
    }

    public static List<LocalDate> addQuarters(LocalDate date, Integer numberOfQuarters) {
        LocalDate endDate = quarterEndFor(date);
        LocalDate startDate = quarterStartFor(date);

        LocalDate beginningDate = startDate.plusMonths(numberOfQuarters * 3);

        LocalDate endingDate = endDate.plusMonths(numberOfQuarters * 3);

        return Arrays.asList(beginningDate, endingDate);
    }

    public static List<LocalDate> subtractQuarters(LocalDate date, Integer numberOfQuarters) {
        LocalDate endDate = quarterEndFor(date);
        LocalDate startDate = quarterStartFor(date);

        LocalDate beginningDate = startDate.minusMonths(numberOfQuarters * 3);

        LocalDate endingDate = endDate.minusMonths(numberOfQuarters * 3);

        return Arrays.asList(beginningDate, endingDate);
    }

    public static List<LocalDate> addMonths(LocalDate date, Integer numberOfMonths) {
        LocalDate workingDate = monthStartFor(date);

        LocalDate addedMonths = workingDate.plusMonths(numberOfMonths);

        return Arrays.asList(monthStartFor(addedMonths), monthEndFor(addedMonths));
    }

    public static List<LocalDate> subtractMonths(LocalDate date, Integer numberOfMonths) {
        LocalDate workingDate = monthStartFor(date);

        LocalDate addedMonths = workingDate.minusMonths(numberOfMonths);

        return Arrays.asList(monthStartFor(addedMonths), monthEndFor(addedMonths));
    }

    public static List<LocalDate> getDatesDuringPeriods(LocalDate workingDate, Integer getPeriodToAdd, Enums.Period period) {
        List<LocalDate> dates;
        if (getPeriodToAdd > 0) {
            if (period == Enums.Period.QUARTERLY) {
                dates = Periods.addQuarters(workingDate, getPeriodToAdd);
            } else if (period == Enums.Period.MONTHLY) {
                dates = Periods.addMonths(workingDate, getPeriodToAdd);
            } else {
                dates = Arrays.asList(workingDate, StubDate.dateOf(DateUtil.formatDate(new Date(), "yyyy-MM-dd")));
            }
        } else {
            if (period == Enums.Period.MONTHLY) {
                dates = Arrays.asList(Periods.monthStartFor(workingDate), Periods.monthEndFor(workingDate));
            } else if (period == Enums.Period.QUARTERLY) {
                dates = Arrays.asList(Periods.quarterStartFor(workingDate), Periods.quarterEndFor(workingDate));
            } else {
                dates = Arrays.asList(workingDate, StubDate.dateOf(DateUtil.formatDate(new Date(), "yyyy-MM-dd")));
            }
        }
        return dates;
    }

}
