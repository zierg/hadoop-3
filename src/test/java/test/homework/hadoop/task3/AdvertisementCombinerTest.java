package test.homework.hadoop.task3;

import homework.hadoop.task3.AdvertisementCombiner;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementCombinerTest {

    @Test
    public void test() throws IOException {
        new ReduceDriver<Text, TempAdvertisementDataWritable, Text, TempAdvertisementDataWritable>()
                .withReducer(new AdvertisementCombiner())
                .withInput(CITY_ID, Arrays.asList(INPUT_1, INPUT_2, INPUT_3))
                .withOutput(CITY_ID, EXPECTED_OUTPUT_1)
                .withOutput(CITY_ID, EXPECTED_OUTPUT_2)
                .runTest(false);
    }

    static Text CITY_ID = new Text("120");

    static int INPUT_1_AMOUNT = 1;
    static int INPUT_2_AMOUNT = 1;
    static int INPUT_3_AMOUNT = 4;

    static int WINDOWS = 0;
    static int NOT_WINDOWS = 1;

    static TempAdvertisementDataWritable INPUT_1 = new TempAdvertisementDataWritable()
            .setOsTypeGroupNumber(WINDOWS)
            .setAmount(INPUT_1_AMOUNT);

    static TempAdvertisementDataWritable INPUT_2 = new TempAdvertisementDataWritable()
            .setOsTypeGroupNumber(WINDOWS)
            .setAmount(INPUT_2_AMOUNT);

    static TempAdvertisementDataWritable INPUT_3 = new TempAdvertisementDataWritable()
            .setOsTypeGroupNumber(NOT_WINDOWS)
            .setAmount(INPUT_3_AMOUNT);

    static TempAdvertisementDataWritable EXPECTED_OUTPUT_1 = new TempAdvertisementDataWritable()
            .setOsTypeGroupNumber(WINDOWS)
            .setAmount(INPUT_1_AMOUNT + INPUT_1_AMOUNT);

    static TempAdvertisementDataWritable EXPECTED_OUTPUT_2 = new TempAdvertisementDataWritable()
            .setOsTypeGroupNumber(NOT_WINDOWS)
            .setAmount(INPUT_3_AMOUNT);
}