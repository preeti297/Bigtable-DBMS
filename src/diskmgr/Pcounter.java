package diskmgr;

public class Pcounter {
    public static int rcounter =0;
    public static int wcounter = 0;

    public static void initialize() {
        rcounter = 0;
        wcounter = 0;
        
    }
    public static void readIncrement() {
        rcounter++;
}

    public static void writeIncrement() {
        wcounter++;
    }
}