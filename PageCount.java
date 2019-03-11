import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCount {

  public static class ProjectionMapper
      extends Mapper<Object, Text, IntWritable, IntWritable>{
    private IntWritable id_venue = new IntWritable();
    private IntWritable number_pages = new IntWritable();

    protected void map(Object key, Text value, Context context
                       ) throws IOException, InterruptedException {
      // Our file contains space-limited values: paper_id, paper_name, venue_id, pages, url
      // We project out: venue_id, pages
      String[] tokens = value.toString().split("\t");
      String[] info_pages = tokens[3].split("-");
      // COMPLETE THIS SECTION
      try {
        if(info_pages.length < 2) {
          this.number_pages.set(1);
        } else {
          String[] begin_pages = info_pages[0].split(":");
          String[] end_pages = info_pages[1].split(":");  // (to take the formatting into account)
          if(begin_pages.length <= 1 && end_pages.length <= 1) {
            this.number_pages.set(Integer.parseInt(end_pages[0])-Integer.parseInt(begin_pages[0]));  // difference between the page numbers
          } else {
            if (begin_pages.length == 2 && end_pages.length == 2) {
              this.number_pages.set(Integer.parseInt(end_pages[1])-Integer.parseInt(begin_pages[1]));   // looking at the page information
            } else {
              this.number_pages.set(1);
            }
            
          }
        }
      } catch(NumberFormatException e) {
        this.number_pages.set(1);
      }
      this.id_venue.set(Integer.parseInt(tokens[2]));
      context.write(this.id_venue, this.number_pages);
    }
  }

  public static class IntSumReducer
      extends Reducer <IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val: values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "page count");
    job.setJarByClass(PageCount.class);
    job.setMapperClass(ProjectionMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
