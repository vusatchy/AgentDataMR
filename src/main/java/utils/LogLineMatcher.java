package utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogLineMatcher implements Cloneable {



    private static class ParserUtils {

        public static Long safeParseLong(String byteSym) {
            try {
                return Long.parseLong(byteSym);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }



    private static final Integer IP_GROUP = 1;
    private static final Integer BYTE_GROUP = 2;
    // private static final int REFFER_GROUP = 3;
    private static final int USER_AGENT_GROUP = 3;;

    // ^(ip[\d]+) \S+ \S+ \[[^\]]*]* "[^"]*" \d\d\d (\w+) ("[^"]*") "([^"]*)"$
    private static String LOG_LINE_REGEX = "^(ip[\\d]+) \\S+ \\S+ \\[[^\\]]*]* \"[^\"]*\" \\d\\d\\d ([^\\s]*) \"[^\"]*\" \"([^\"]*)\"$";

    private Pattern LOG_LINE_REGEX_PATTERN = Pattern.compile(LOG_LINE_REGEX);

    private Matcher lineMatcher = null;

    private boolean isLineMatched;

    public void  matchLine(String apacheLogLine) {
        lineMatcher = LOG_LINE_REGEX_PATTERN.matcher(apacheLogLine);
        isLineMatched = lineMatcher.find();
    }

    public String getIp() {
        return lineMatcher.group(IP_GROUP);
    }

    public String getBytesSymbols() {
        return lineMatcher.group(BYTE_GROUP);
    }

    public Long getBytes() {
        return ParserUtils.safeParseLong(lineMatcher.group(BYTE_GROUP));
    }

    public String getUserAgent() {
        return lineMatcher.group(USER_AGENT_GROUP);
    }

    public boolean isValidLine() {
        return isLineMatched;
    }


}
