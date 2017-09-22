package mapper;

import browsers.Browsers;
import custom_writable.TotalAndCount;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.LogLineMatcher;

import java.io.IOException;

public class AgentDataMapper extends Mapper<LongWritable, Text, Text, TotalAndCount> {
     private final int one=1;

    @Override
    protected void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException {
        LogLineMatcher logLineMatcher = new LogLineMatcher();
        logLineMatcher.matchLine(value.toString());

        String br =UserAgent.parseUserAgentString(logLineMatcher.getUserAgent()).getBrowser().getGroup().getName();
        Browsers browsers = Browsers.find(br);
        if(browsers!=null) context.getCounter(browsers).increment(one);

        String ip =logLineMatcher.getIp();
        Long bytes=logLineMatcher.getBytes();
        if(ip!=null && bytes!=null) context.write(new Text(ip),new TotalAndCount(bytes.longValue(),one));


    }
}
