package combiner;

import custom_writable.TotalAndCount;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AgentDataCombiner extends Reducer<Text, TotalAndCount, Text,TotalAndCount> {
    @Override
    protected void reduce(Text key, Iterable<TotalAndCount> values, Context context) throws IOException, InterruptedException {
        int total=0;
        int counter=0;
        for(TotalAndCount v:values){
            TotalAndCount tac=new TotalAndCount(v);
            total+=tac.getTotal();
            counter+=tac.getCount();
        }
        context.write(new Text(key),new TotalAndCount(total,counter));
    }
}
