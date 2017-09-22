import combiner.AgentDataCombiner;
import custom_writable.TotalAndCount;
import mapper.AgentDataMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.AgentDataReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AgentDatatTests {

    MapDriver<LongWritable, Text, Text, TotalAndCount> mapDriver;
    ReduceDriver<Text, TotalAndCount, Text, Text> reduceDriver;
    ReduceDriver<Text, TotalAndCount, Text, TotalAndCount> combineDriver;
    MapReduceDriver<LongWritable, Text, Text, TotalAndCount, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        AgentDataMapper mapper = new AgentDataMapper();
        AgentDataReducer reducer = new AgentDataReducer();
        AgentDataCombiner combiner = new AgentDataCombiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombiner() throws IOException {
        final  String key="ip666";
        List<TotalAndCount> values = new ArrayList<>();
        values.add(new TotalAndCount(100,1));
        values.add(new TotalAndCount(300,3));
        combineDriver.withInput(new Text(key), values);
        combineDriver.withOutput(new Text(key),new TotalAndCount(400,4));
        combineDriver.runTest();
    }

    @Test
    public void testMapper() throws IOException {
        final String testLine = "ip37 - - [24/Apr/2011:06:18:08 -0400] \"GET /sun_ipc/IPC_led.jpg HTTP/1.0\" 200 2197 \"http://host2/sun_ipc/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
        mapDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapDriver.withOutput(new Text("ip37"), new TotalAndCount(2197, 1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        final  String key="ip666";
        List<TotalAndCount> values = new ArrayList<>();
        values.add(new TotalAndCount(100,1));
        values.add(new TotalAndCount(300,3));
        reduceDriver.withInput(new Text(key), values);
        reduceDriver.withOutput(new Text(key),new Text(100.0+" , "+400));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        final String testLine = "ip37 - - [24/Apr/2011:06:18:08 -0400] \"GET /sun_ipc/IPC_led.jpg HTTP/1.0\" 200 2197 \"http://host2/sun_ipc/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
        mapReduceDriver.withInput(new LongWritable(1), new Text(new String(testLine)));
        mapReduceDriver.withOutput(new Text("ip37"),new Text(2197.0+" , "+2197));
        mapReduceDriver.runTest();

    }

}