package homework.hadoop.task3.mapping;

import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementMapper extends Mapper<Object, Text, TempAdvertisementDataWritable, IntWritable> {

    static Logger log = LoggerFactory.getLogger(AdvertisementMapper.class);
}
