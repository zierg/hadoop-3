package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementCombiner extends Reducer<Text, TempAdvertisementDataWritable, Text, TempAdvertisementDataWritable> {

    static Logger log = LoggerFactory.getLogger(AdvertisementCombiner.class);
}
