package browsers;


import java.util.Arrays;

public enum Browsers {

    CHROME("Chrome"),
    RS("Robot/Spider"),
    FIREFOX("Firefox"),
    IE("Internet Explorer"),
    OPERA("Opera"),
    UNKNOWN("Unknown"),
    SAFARI("Safari");

    private final String name;

    private Browsers(String s) {
        name = s;
    }

    public static Browsers find(String val) {
        return Arrays.stream(Browsers.values())
                .filter(e -> e.name.equals(val))
                .findAny()
                .orElse(null);
    }
}

