package homework.hadoop.task3.mapping;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@SuppressWarnings("WeakerAccess")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class UserAgentUtils {

    public static String getOsType(String userAgent) {
        UserAgent agent = UserAgent.parseUserAgentString(userAgent);
        return agent.getOperatingSystem().getGroup().getName();
    }

    private UserAgentUtils() {}
}