package custom_writable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TotalAndCount implements WritableComparable<TotalAndCount>{


    private LongWritable total;
    private IntWritable count;


    public  TotalAndCount(){

    }

    public TotalAndCount(long total, int count) {
        this.total = new LongWritable(total);
        this.count = new IntWritable(count);
    }

    public TotalAndCount(TotalAndCount v) {
        this.count=v.count;
        this.total=v.total;
    }


    public long getTotal() {
        return total.get();
    }

    public int getCount() {
        return count.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(total.get());
        dataOutput.writeInt(count.get());

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total =  new LongWritable(dataInput.readLong());
        count = new IntWritable(dataInput.readInt());

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TotalAndCount)) return false;

        TotalAndCount that = (TotalAndCount) o;

        if (getTotal() != that.getTotal()) return false;
        return getCount() == that.getCount();
    }

    @Override
    public int hashCode() {
        int result = (int) (getTotal() ^ (getTotal() >>> 32));
        result = 31 * result + getCount();
        return result;
    }

    @Override
    public int compareTo(TotalAndCount o) {
        return total.compareTo(o.total);
    }
}
