import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class PageRank {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double INITIAL_PR = 1.0;

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) return;

            String node = parts[0];
            String[] adjacencyList = parts[1].split(",");
            double pageRank = Double.parseDouble(parts[2]);

            for (String neighbor : adjacencyList) {
                context.write(new Text(neighbor), new Text("PR:" + (pageRank / adjacencyList.length)));
            }
            
            // Emit adjacency list to retain graph structure
            context.write(new Text(node), new Text("LIST:" + parts[1]));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            String adjacencyList = "";

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("PR:")) {
                    sum += Double.parseDouble(str.substring(3));
                } else if (str.startsWith("LIST:")) {
                    adjacencyList = str.substring(5);
                }
            }

            double newPageRank = (1 - DAMPING_FACTOR) + (DAMPING_FACTOR * sum);
            context.write(key, new Text(adjacencyList + "\t" + newPageRank));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
