import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
public static class CustomerMapper extends Mapper <Object, Text, Text, Text>
{
public void map(Object key, Text value, Context context) throws IOException, InterruptedException
{String record = value.toString();
String[] parts = record.split(",");
context.write(new Text(parts[0]), new Text("state\t" + parts[4]));   // we need Customer_Id and State column 
}
}

public static class SalesMapper extends Mapper <Object, Text, Text, Integer>
{
public void map(Object key, Text value, Context context) throws IOException, InterruptedException
{
String record = value.toString();
String[] parts = record.split(",");
context.write(new Text(parts[0]), new Text("Sales_Price\t" + parts[2]));
}
}

public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Integer>
{
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException 
{

Integer total = 0;

for (Text t : values) 
{ 
String parts[] = t.toString().split("\t");
if (parts[0].equals("Sales_Price")) 
{

total += Integer.parseInt(parts[1]);
} 

}
context.write(new Text(state), new Text(total));
}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "Reduce-side join");
job.setJarByClass(ReduceJoin.class);
job.setReducerClass(ReduceJoinReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);


MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, Customer.class);
MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, Sales.class);
Path outputPath = new Path(args[2]);


FileOutputFormat.setOutputPath(job, outputPath);
outputPath.getFileSystem(conf).delete(outputPath);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
