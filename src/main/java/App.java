import browsers.Browsers;
import combiner.AgentDataCombiner;
import custom_writable.TotalAndCount;
import eu.bitwalker.useragentutils.UserAgent;
import mapper.AgentDataMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import reducer.AgentDataReducer;
import utils.LogLineMatcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration c = new Configuration();
        c.set("mapred.textoutputformat.separatorText", ",");
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Job j = new Job(c, "AD");
        j.setJarByClass(App.class);
        j.setMapperClass(AgentDataMapper.class);
        j.setCombinerClass(AgentDataCombiner.class);
        j.setReducerClass(AgentDataReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TotalAndCount.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
       // j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);


    }

}
