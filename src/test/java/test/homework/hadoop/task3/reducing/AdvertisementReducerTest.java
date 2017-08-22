package test.homework.hadoop.task3.reducing;

import homework.hadoop.task3.reducing.AdvertisementReducer;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementReducerTest {

    @Test
    public void test() throws IOException {
        new ReduceDriver<Text, TempAdvertisementDataWritable, Text, IntWritable>()
                .withReducer(new AdvertisementReducer())
                .withInput(CITY_ID, Arrays.asList(INPUT_1, INPUT_2, INPUT_3))
                .withOutput(CITY_ID, TOTAL_AMOUNT)
                .runTest(false);
    }

    static Text CITY_ID = new Text("120");

    static int INPUT_1_AMOUNT = 1;
    static int INPUT_2_AMOUNT = 1;
    static int INPUT_3_AMOUNT = 4;

    static IntWritable TOTAL_AMOUNT = new IntWritable(INPUT_1_AMOUNT + INPUT_2_AMOUNT + INPUT_3_AMOUNT);

    static TempAdvertisementDataWritable INPUT_1 = new TempAdvertisementDataWritable()
            .setAmount(INPUT_1_AMOUNT);

    static TempAdvertisementDataWritable INPUT_2 = new TempAdvertisementDataWritable()
            .setAmount(INPUT_2_AMOUNT);

    static TempAdvertisementDataWritable INPUT_3 = new TempAdvertisementDataWritable()
            .setAmount(INPUT_3_AMOUNT);
}