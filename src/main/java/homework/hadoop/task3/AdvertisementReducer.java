package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementReducer extends Reducer<Text, TempAdvertisementDataWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<TempAdvertisementDataWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (val value : values) {
            total += value.getAmount().get();
        }
        totalAmount.set(total);
        context.write(key, totalAmount);
    }

    IntWritable totalAmount = new IntWritable();

    static Logger log = LoggerFactory.getLogger(AdvertisementReducer.class);
}
