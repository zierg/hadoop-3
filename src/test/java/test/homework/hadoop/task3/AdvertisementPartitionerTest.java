package test.homework.hadoop.task3;

import homework.hadoop.task3.AdvertisementPartitioner;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class AdvertisementPartitionerTest {

    @Parameterized.Parameter
    public int osTypeGroupNumber;

    @Parameterized.Parameter(1)
    public int numPartitions;

    @Parameterized.Parameter(2)
    public int expectedPartition;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][] {
                new Object[] {0, 2, 0},
                new Object[] {1, 2, 1},
                new Object[] {1, 1, 0}
        };
    }

    @Test
    public void testZeroNumberAndTwoPartitions() {
        int partition = tested.getPartition(
                new Text(),
                new TempAdvertisementDataWritable().setOsTypeGroupNumber(osTypeGroupNumber),
                numPartitions);
        assertThat(partition).isEqualTo(expectedPartition);
    }

    AdvertisementPartitioner tested = new AdvertisementPartitioner();
}