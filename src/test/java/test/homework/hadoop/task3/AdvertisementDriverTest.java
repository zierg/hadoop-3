package test.homework.hadoop.task3;

import homework.hadoop.task3.AdvertisementCombiner;
import homework.hadoop.task3.AdvertisementReducer;
import homework.hadoop.task3.TempAdvertisementDataWritable;
import homework.hadoop.task3.mapping.AdvertisementMapper;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementDriverTest {

    @Test
    public void test() throws IOException {
        MapReduceDriver.<Object, Text, Text, TempAdvertisementDataWritable, Text, IntWritable>newMapReduceDriver()
                .withMapper(new AdvertisementMapper())
                .withCombiner(new AdvertisementCombiner())
                .withReducer(new AdvertisementReducer())
                .withInput(new LongWritable(0), new Text(TEST))
                .withOutput(new Text("0"), new IntWritable(3))
                .withOutput(new Text("1"), new IntWritable(1))
                .withOutput(new Text("16"), new IntWritable(1))
                .withOutput(new Text("79"), new IntWritable(2))
                .withOutput(new Text("149"), new IntWritable(1))
                .withOutput(new Text("202"), new IntWritable(1))
                .withOutput(new Text("393"), new IntWritable(1))
                .runTest(false);
    }

    private static final String TEST = "a80fc47c7b544c78eb54a793e72a198e\t20131027170602323\t1\tCAV6UQBndoT\tMozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0\t10.237.95.*\t0\t0\t1\td2c2382e14b7d7de3a32218dba3192b1\t503afcab3679835909f87eedbc559b0\tnull\tmm_45472847_4168669_13664047\t950\t90\tNa\tNa\t0\t12628\t294\t287\tnull\t2261\t13866\n" +
            "1a0d305412705a4582046022a2936998\t20131027145600419\t1\tCB2IRSDqdo4\tMozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)\t101.224.101.*\t79\t79\t2\t8b6dd03ea345902acb8bfab9758b7f41\td39f887428c82dfd31bf0cce78edfff4\tnull\t1120607322\t200\t200\tOtherView\tNa\t5\t12616\t277\t23\tnull\t2261\t10684,10093,10075,13042,10006,13866,10110,13776,10052,10133,10063\n" +
            "16926e1d870f50ecc594ac0fd84bf56a\t20131027105000824\t1\tCB43lg1ecfV\tMozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0\t1.171.177.*\t393\t393\t1\t3ad536e5bd3cb65a4312c3eea66b6911\t42c8376779bbb6e8aca33bf6152cfa8d\tnull\tmm_10075660_3500949_11453278\t950\t90\tNa\tNa\t0\t12628\t294\t261\tnull\t2261\t13866\n" +
            "37956b2c5de9f931a772f6e784908e74\t20131027115700283\t1\tCBJMQM4jc4d\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t124.77.8.*\t79\t79\t2\taedc85a495b5fc0aaa4f5ac9254b7372\t8ed8ac1fae7872e8951bc56840761da6\tnull\t3175797632\t120\t600\tOtherView\tNa\t180\t12611\t277\t180\tnull\t2261\t10057,10059,10075,10083,10129,13866,10006,10110,10146,10126,13403,10063,10116\n" +
            "ec2bbca486bfb5a1a7724daac66a394\t20131027171900361\t1\tCBKHZB0EeBf\tMozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)\t112.227.88.*\t146\t149\t2\t4d279e1fd14d57521c224cce7a94ce4d\t88f0d52291cb99d7b04380261f5871f7\tnull\t2545335729\t728\t90\tOtherView\tNa\t4\t12627\t277\t153\tnull\t2261\t10102,11423,10131,10063,11092\n" +
            "c9be3f26819b392682b133b883ddfc9\t20131027131001147\t1\tCBRL6I1rdg8\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t110.203.13.*\t201\t202\t2\t4fb43bc8c5f389dffeed16f2ed93dba1\t4a4bf45792818b07a76480509a6057de\tnull\t740866086\t120\t240\tOtherView\tNa\t5\t12610\t277\t65\tnull\t2261\t10057,13800,10059,13496,10076,10077,10093,10075,10083,10102,13866,10024,10006,10031,10111,10146,10120,10131,10052,13403,10115,10063\n" +
            "72afaeab13c6aebc4f279dd948578aa9\t20131027022500239\t1\tCBTHbG9KbPq\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0\t61.48.210.*\t1\t1\t2\tc6269115a4867b6ea008314256b67b53\tnull\tnull\t3696374687\t728\t90\tOtherView\tNa\t4\t12627\t277\t53\tnull\t2261\t10048,10057,13800,16593,10684,10076,10077,10117,10075,10093,10129,10102,10024,10006,13866,10110,13776,10145,13403,10142,10063,10125\n" +
            "bebc122fbc93075e79baa58b909e5d1f\t20131027164904712\t1\tCBU6ri9beXA\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31\t193.105.7.*\t0\t0\t3\tdd4270481b753dde29898e27c7c03920\t2ff099814ab02af3633e04e15c6bb833\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t12633\t294\t70\tnull\t2261\t13800,10076,10006,10146,10133,10063\n" +
            "9e5c653b6534222c856f2d3c29ef2ca9\t20131027111801462\t1\tCC1Eo9Bob3c\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; InfoPath.3)\t171.116.58.*\t15\t16\t1\tda5060926e3fd2ac022b83172025db08\tcffbf42d00a7ccb4014aa6f16df1c60\tnull\tmm_34618856_4222645_14290379\t640\t90\tNa\tNa\t0\t12626\t294\t125\tnull\t2261\t10079,10075,10006,10148,10063,10116\n" +
            "88af4651329c8c5efd027e825d0a8e28\t20131027144716787\t1\tDAREkGCJwFJ\tMozilla/5.0 (iPad; U; CPU OS 5_0 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J3 Safari/6533.18.5\t76.91.64.*\t0\t0\t3\te96dd4d612a79976fd4155437b0be59a\t649fc5cdef0923f8600444e195f2d223\tnull\tQQlive_SP_HP_bottom_Width\t980\t90\tNa\tNa\t20\t12632\t294\t55\tnull\t2261\tnull";
}