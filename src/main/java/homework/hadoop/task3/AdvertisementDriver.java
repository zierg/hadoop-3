package homework.hadoop.task3;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementDriver {

    public static void main(String[] args) {
        UserAgent agent = UserAgent.parseUserAgentString("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0");
        log.info("OS: {}", agent.getOperatingSystem().getGroup().getName());
    }

    static Logger log = LoggerFactory.getLogger(AdvertisementDriver.class);
}
