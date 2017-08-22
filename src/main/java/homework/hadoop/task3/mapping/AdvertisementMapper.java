package homework.hadoop.task3.mapping;

import homework.hadoop.task3.OsTypeDivider;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementMapper extends Mapper<Object, Text, Text, TempAdvertisementDataWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        setupThreshold(context);
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            processAdvertisementRecord(itr.nextToken(), context);
        }
    }

    private void setupThreshold(Context context) {
        bidPriceThreshold = context.getConfiguration().getInt("homework.hadoop.task3.bidding-price-threshold", BID_PRICE_DEFAULT_THRESHOLD);
    }

    private void processAdvertisementRecord(String record, Context context) throws IOException, InterruptedException {
        ParamsExtractor.Params params = ParamsExtractor.getAdvertisementParams(record);
        context.getCounter("Bid", Objects.toString(params.getBidPrice())).increment(1);
        if (params.getBidPrice() > bidPriceThreshold) {
            write(params, context);
        }
    }

    private void write(ParamsExtractor.Params params, Context context) throws IOException, InterruptedException {
        advertisementData.setAmount(1);
        int osTypeGroupNumber = getOsTypeGroupNumber(params);
        context.getCounter("OS Statistics", Objects.toString(osTypeGroupNumber)).increment(1);
        advertisementData.setOsTypeGroupNumber(osTypeGroupNumber);
        cityId.set(params.getCityId());
        context.write(cityId, advertisementData);
    }

    private int getOsTypeGroupNumber(ParamsExtractor.Params params) {
        String osType = UserAgentUtils.getOsType(params.getUserAgent());
        return OsTypeDivider.getNumberForOs(osType);
    }

    @NonFinal
    int bidPriceThreshold;

    Text cityId = new Text();
    TempAdvertisementDataWritable advertisementData = new TempAdvertisementDataWritable();

    static int BID_PRICE_DEFAULT_THRESHOLD = 250;

    static Logger log = LoggerFactory.getLogger(AdvertisementMapper.class);
}
