package utility;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

public class Date_Parser {

    /**
     * Function to obtain the Calendar from date
     * @param convertedCurrentDate to set calendar time from
     * @return Calendar with time set
     */
    public static Calendar getCalendar(Date convertedCurrentDate){
        Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
        assert convertedCurrentDate != null;
        calendar.setTime(convertedCurrentDate);
        return calendar;
    }

    /**
     * Function to obtain the date from the starting format
     * @param date to parse
     * @param startFormat to which date has to be parsed
     * @return converted date
     */
    public static Date getConvertedDate(String date, String startFormat){
        SimpleDateFormat sdf = new SimpleDateFormat(startFormat);
        Date convertedCurrentDate = null;
        try {
            convertedCurrentDate = sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return convertedCurrentDate;
    }

    /**
     * Function to get the string date formatted
     * @param calendar with time set
     * @param finalFormat in which to have the date
     * @return the string formatted
     */
    public static String getDate(Calendar calendar, String finalFormat){
        SimpleDateFormat monthDate = new SimpleDateFormat(finalFormat);
        return monthDate.format(calendar.getTime());
    }

    /**
     * Function to calculate the number of days in a month
     * @param calendar containing the time set
     * @return the number of days
     */
    public static Double getDaysInMonth(Calendar calendar){
        int month = calendar.get(Calendar.MONTH)+1;
        YearMonth yearMonthObject = YearMonth.of(calendar.get(Calendar.YEAR), month);
        return (double) yearMonthObject.lengthOfMonth();
    }

}
