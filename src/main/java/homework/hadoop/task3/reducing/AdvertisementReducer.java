package homework.hadoop.task3.reducing;

import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementReducer extends Reducer<Text, TempAdvertisementDataWritable, Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();
        if (containsCitiesCache(cacheFiles)) {
            URI cacheFile = cacheFiles[0];
            cities = new CitiesCacheFileParser(cacheFile, context).getCities();
        } else {
            log.warn("There is no cache file with cities");
            cities = Collections.emptyMap();
        }
    }

    private boolean containsCitiesCache(URI[] cacheFiles) {
        return cacheFiles != null && cacheFiles.length == 1;
    }

    @Override
    protected void reduce(Text key, Iterable<TempAdvertisementDataWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (val value : values) {
            total += value.getAmount().get();
        }
        setValues(key, total);
        context.write(city, totalAmount);
    }

    private void setValues(Text key, int total) {
        totalAmount.set(total);
        String keyString = key.toString();
        city.set(cities.getOrDefault(keyString, keyString));
    }

    @NonFinal
    Map<String, String> cities;

    IntWritable totalAmount = new IntWritable();
    Text city = new Text();

    static Logger log = LoggerFactory.getLogger(AdvertisementReducer.class);
}
