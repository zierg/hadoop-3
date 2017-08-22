package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementReducer extends Reducer<Text, TempAdvertisementDataWritable, Text, IntWritable> {

    static Logger log = LoggerFactory.getLogger(AdvertisementReducer.class);
}
