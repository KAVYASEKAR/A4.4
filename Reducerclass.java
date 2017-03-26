import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Reducerclass extends Reducer<Text, IntWritable, Text, IntWritable>
{
	IntWritable count = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int n = 0;
		for (IntWritable value : values) {
			n=n+value.get();
	}
		count.set(n);
		context.write(key, count);
	}
}