package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementCombiner extends Reducer<Text, TempAdvertisementDataWritable, Text, TempAdvertisementDataWritable> {

    @Override
    protected void reduce(Text key, Iterable<TempAdvertisementDataWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> combined = new HashMap<>();

        for (val value : values) {
            reducePerOs(combined, value);
        }

        for (Map.Entry<String, Integer> entry : combined.entrySet()) {
            output.setOsType(entry.getKey());
            output.setAmount(entry.getValue());
            context.write(key, output);
        }
    }

    private void reducePerOs(Map<String, Integer> combined, TempAdvertisementDataWritable value) {
        combined.computeIfPresent(value.getOsType().toString(), (os, amount) -> amount += value.getAmount().get());
        combined.computeIfAbsent(value.getOsType().toString(), (os) -> value.getAmount().get());
    }

    TempAdvertisementDataWritable output = new TempAdvertisementDataWritable();

    static Logger log = LoggerFactory.getLogger(AdvertisementCombiner.class);
}
