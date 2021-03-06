package assignment1b.part1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
/**
* @author Radhika Kalaiselvan
*/
public class WordCount extends Configured implements Tool {
  
  public static void main(String[] args) throws Exception {
	
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(IntWritable.class);
    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
    job.getConfiguration().set("stopWordsFile", args[2]);
    return job.waitForCompletion(true) ? 0 : 1;
    
  }

  
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private boolean caseSensitive = false;
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    
    
    private void parseSkipFile(String patternsURI) {
	    try {
	    	Path pt=new Path(patternsURI);//Location of file in HDFS-"hdfs:/path/to/file"
	        FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader fis =new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String pattern;
	        while ((pattern = fis.readLine()) != null) {
	          patternsToSkip.add(pattern);
	        }
	      } catch (IOException ioe) {
	        System.err.println("Caught exception while parsing the stop words file '"
	            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
	      }
	    }
    
    @Override
    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      String path = config.get("stopWordsFile");
      parseSkipFile(path);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
     
    //convert the line to lower case
     line = line.toLowerCase();
      
      
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
        	//for each word in the line check if it is not special character, len >=5 and not in stop words
          if (word.isEmpty() || patternsToSkip.contains(word)|| word.matches("\\b\\W*\\b") || word.matches("\\b\\d*\\b") || word.matches("\\b.{1,4}\\b") )  {
            continue;
          }
          if (! word.matches("\\b\\w*\\b") )  {
              continue;
            }
            
          currentWord = new Text(word);
          context.write(currentWord,one);
        }         
      }
  }

  //Add the word count and write word, count to the context
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}