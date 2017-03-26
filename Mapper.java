import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 


public class AssignMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text gen = new Text();
	IntWritable o = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		int i = Integer.parseInt(a[1]);
		if(i==1){
			Gender.set(a[4]);
			context.write(gen, o);
		}
	}
}
