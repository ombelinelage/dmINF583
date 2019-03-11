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

public class Top5 {

  public static class ProjectionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable id_author = new IntWritable();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // used to gather the different values of id_author
      String[] line_author = value.toString().split("\t");
      id_author.set(Integer.parseInt(line_author[1]));
      context.write(id_author, new IntWritable(1));
    }
  }

  public static class Top5Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private int[] val_top5 = new int[5];
    private int[] id_author_top5 = new int[5];

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int value = 0;
      for (IntWritable val : values) {
        value += val.get();
      }

      if (value >= val_top5[0]) {
        this.id_author_top5[0] = key.get();
        this.val_top5[0] = value;

        int i = 0;
        int tmp;
        while (i < 4 && val_top5[i + 1] < val_top5[i]) {
          tmp = this.id_author_top5[i];
          this.id_author_top5[i] = this.id_author_top5[i + 1];
          this.id_author_top5[i + 1] = tmp;

          tmp = val_top5[i];
          this.val_top5[i] = this.val_top5[i + 1];
          this.val_top5[i + 1] = tmp;

          i++;
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = 0; i < this.id_author_top5.length; i++) {
        context.write(new IntWritable(this.id_author_top5[i]), new IntWritable(this.val_top5[i]));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "author publication count");
    job.setJarByClass(Top5.class);
    job.setMapperClass(ProjectionMapper.class);
    job.setCombinerClass(Top5Reducer.class);
    job.setReducerClass(Top5Reducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
