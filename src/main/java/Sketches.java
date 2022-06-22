import utils.Tools;

import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Locale;

public class Sketches {
    public static void main(String[] args) throws ParseException {
//        String string_tmp = "12,4000";
//        Double tmp1 = NumberFormat.getInstance(Locale.getDefault()).parse(string_tmp).doubleValue();
//        Double tmp2 = utils.Tools.stringToDouble(string_tmp);
//        System.out.println("String: " + string_tmp);
//        System.out.println("Double (NF): " + tmp1);
//        System.out.println("Double (SD): " + tmp2);

        System.out.println(Tools.getSecondsSlot(Timestamp.valueOf("2022-05-01 00:00:03"),5));
    }
}