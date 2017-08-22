package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementPartitioner extends Partitioner<Text, TempAdvertisementDataWritable> {

    @Override
    public int getPartition(Text text, TempAdvertisementDataWritable writable, int numPartitions) {
        int osGroupNumber = writable.getOsTypeGroupNumber().get();
        return osGroupNumber < numPartitions ? osGroupNumber : osGroupNumber % numPartitions;
    }
}
