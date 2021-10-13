package miu.cs522.part1.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author XIII
 */
public class ApacheLogParser implements IParser<ApacheLogRecord> {

    private static final String LOG_REGEX = "^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"(.+?)\" (\\d{3}) (\\d+)$";
    private static final Pattern pattern = Pattern.compile(LOG_REGEX);

    private ApacheLogParser() {}

    public static ApacheLogParser getInstance() {
        return LogParserHolder.INSTANCE;
    }

    @Override
    public ApacheLogRecord parse(String s) {
        final Matcher matcher = pattern.matcher(s.trim());

        if (matcher.find()) {
            return new ApacheLogRecord.Builder()
                    .withIpAddress(matcher.group(1))
                    .withBytesSent(Integer.valueOf(matcher.group(7)))
                    .build();
        }

        return null;
    }

    private static class LogParserHolder {

        private static final ApacheLogParser INSTANCE = new ApacheLogParser();
    }
}
