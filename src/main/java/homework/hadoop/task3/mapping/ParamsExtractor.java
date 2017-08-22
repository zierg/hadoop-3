package homework.hadoop.task3.mapping;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;

@SuppressWarnings("WeakerAccess")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class ParamsExtractor {

    public static Params getAdvertisementParams(String advertisementRecord) {
        String[] allParams = advertisementRecord.split("\t");
        return new Params()
                .setUserAgent(allParams[USER_AGENT_ORDER_NUMBER])
                .setCityId(allParams[CITY_ID_ORDER_NUMBER])
                .setBidPrice(Integer.parseInt(allParams[BID_PRICE_ORDER_NUMBER]));
    }

    private ParamsExtractor() {}

    static int USER_AGENT_ORDER_NUMBER = 4;
    static int CITY_ID_ORDER_NUMBER = 7;
    static int BID_PRICE_ORDER_NUMBER = 19;

    @Data
    @Accessors(chain = true)
    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class Params {
        String userAgent;
        String cityId;
        int bidPrice;
    }
}
