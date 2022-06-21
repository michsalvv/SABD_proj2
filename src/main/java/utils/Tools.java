package utils;

public class Tools {

    public static Double stringToDouble(String s) {
        String r = s.replace(",",".");
        return Double.parseDouble(r);
    }
}
