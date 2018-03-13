package assignment1b.part2;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
* @author Radhika Kalaiselvan
*/
public class MovieRatingFinder {

  public static class TokenizerMapper
       extends Mapper<Object, Text,IntWritable, DoubleWritable>{

    private IntWritable k= new IntWritable();
    private DoubleWritable fw=new DoubleWritable();

     
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String val=value.toString();
      String[] field = val.split(",");
    //Split the values
      if(field.length==4 && !val.contains("movieId")){
    	  //If all 4 fields are present then write movie id and ratings to context
    	  k.set(Integer.parseInt(field[1]));
    	  fw.set(Double.parseDouble(field[2]));
    	  context.write(k,fw);
      }
    }
  }

  public static class AverageReducer
       extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
    private DoubleWritable dw=new DoubleWritable();
    private static DecimalFormat df2 = new DecimalFormat(".##");
    
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Double sum = 0D;
      int count=0;
      //add all the reviews.
      //maintain count variable to track the number of reviews
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }
      //write the average to the context
      String value=df2.format(sum/count);
      dw.set(Double.valueOf(value));
      context.write(key,dw);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "movie rating");
    job.setJarByClass(MovieRatingFinder.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
