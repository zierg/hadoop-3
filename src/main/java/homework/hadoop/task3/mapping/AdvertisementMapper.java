package homework.hadoop.task3.mapping;

import homework.hadoop.task3.OsTypeDivider;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementMapper extends Mapper<Object, Text, Text, TempAdvertisementDataWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            processAdvertisementRecord(itr.nextToken(), context);
        }
    }

    private void processAdvertisementRecord(String record, Context context) throws IOException, InterruptedException {
        ParamsExtractor.Params params = ParamsExtractor.getAdvertisementParams(record);
        if (params.getBidPrice() > BID_PRICE_THRESHOLD) {
            write(params, context);
        }
    }

    private void write(ParamsExtractor.Params params, Context context) throws IOException, InterruptedException {
        advertisementData.setAmount(1);
        advertisementData.setOsTypeGroupNumber(getOsTypeGroupNumber(params));
        cityId.set(params.getCityId());
        context.write(cityId, advertisementData);
    }

    private int getOsTypeGroupNumber(ParamsExtractor.Params params) {
        String osType = UserAgentUtils.getOsType(params.getUserAgent());
        return OsTypeDivider.getNumberForOs(osType);
    }

    Text cityId = new Text();
    TempAdvertisementDataWritable advertisementData = new TempAdvertisementDataWritable();

    static int BID_PRICE_THRESHOLD = 250;

    static Logger log = LoggerFactory.getLogger(AdvertisementMapper.class);
}
