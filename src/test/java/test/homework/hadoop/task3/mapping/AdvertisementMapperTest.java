package test.homework.hadoop.task3.mapping;

import homework.hadoop.task3.TempAdvertisementDataWritable;
import homework.hadoop.task3.mapping.AdvertisementMapper;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementMapperTest {

    @Test
    public void test() throws IOException {
        Text value = new Text(TEST);
        new MapDriver<Object, Text, Text, TempAdvertisementDataWritable>()
                .withMapper(new AdvertisementMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(EXPECTED_OUTPUT_KEY, EXPECTED_OUTPUT_VALUE)
                .runTest(false);
    }

    static Text EXPECTED_OUTPUT_KEY = new Text("159");

    static TempAdvertisementDataWritable EXPECTED_OUTPUT_VALUE = new TempAdvertisementDataWritable()
            .setAmount(1)
            .setOsTypeGroupNumber(0);

    static String TEST = "481a3572b254fac0a100b3264ae0dff0\t20131027105900443\t1\tCC3FELF7dBw\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0\t112.251.126.*\t146\t159\t2\t13625cb070ffb306b425cd803c4b7ab4\td2a55a0aaf1e75231290f9369b711aa1\tnull\t127598132\t728\t90\tOtherView\tNa\t133\t12627\t277\t133\tnull\t2261\t13800,10117,10102,10024,10006,10110,10133,10063\n" +
            "88af4651329c8c5efd027e825d0a8e28\t20131027144716787\t1\tDAREkGCJwFJ\tMozilla/5.0 (iPad; U; CPU OS 5_0 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J3 Safari/6533.18.5\t76.91.64.*\t0\t0\t3\te96dd4d612a79976fd4155437b0be59a\t649fc5cdef0923f8600444e195f2d223\tnull\tQQlive_SP_HP_bottom_Width\t980\t90\tNa\tNa\t20\t12632\t194\t55\tnull\t2261\tnull";
}