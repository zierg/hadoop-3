package homework.hadoop.task3;

import homework.hadoop.task3.mapping.AdvertisementMapper;
import homework.hadoop.task3.reducing.AdvertisementReducer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * How to use
 * Run in local mode: java -jar homework-3-advertisement-1.0-SNAPSHOT-all.jar input output
 * Run on a cluster: hadoop jar homework-3-advertisement-1.0-SNAPSHOT-all.jar input output city.en.txt
 * Run on a cluster with 280 Bidding Price threshold: hadoop jar homework-3-advertisement-1.0-SNAPSHOT-all.jar input output city.en.txt 280
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        AdvertisementDriver driver = new AdvertisementDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        setupMapperParameters(args, conf);
        Job job = Job.getInstance(conf, "Advertisement Data");
        job.setJarByClass(AdvertisementDriver.class);
        job.setMapperClass(AdvertisementMapper.class);
        job.setCombinerClass(AdvertisementCombiner.class);
        job.setReducerClass(AdvertisementReducer.class);
        job.setPartitionerClass(AdvertisementPartitioner.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TempAdvertisementDataWritable.class);
        job.setOutputValueClass(IntWritable.class);
        setupCache(args, job);
        setupFileSystem(conf, job, args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void setupCache(String[] args, Job job) throws IOException {
        if (args.length >= 3) {
            Path path = new Path(args[2]);
            boolean exists = FileSystem.getLocal(job.getConfiguration()).exists(path);
            if (exists) {
                job.addCacheFile(path.toUri());
            }
        }
    }

    private void setupMapperParameters(String[] args, Configuration conf) {
        if (args.length >= 4) {
            try {
                int threshold = Integer.parseInt(args[3]);
                conf.setInt("homework.hadoop.task3.bidding-price-threshold", threshold);
            } catch (NumberFormatException ex) {
                log.error("Incorrect value for the Bidding Price Threshold");
            }
        }
    }

    private void setupFileSystem(Configuration conf, Job job, String[] args) throws IOException {
        FileInputFormat.addInputPath(job, new Path(args[0]));
        val outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
    }

    static Logger log = LoggerFactory.getLogger(AdvertisementDriver.class);
}
