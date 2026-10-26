
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AveragePM25 {

    public static class PMMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty() || line.startsWith("City,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            if (fields.length != 2 || fields[0].trim().isEmpty()) {
                context.getCounter("Data Quality", "Invalid Records").increment(1);
                return;
            }

            try {
                double pm25 = Double.parseDouble(fields[1].trim());

                if (!Double.isFinite(pm25) || pm25 < 0) {
                    context.getCounter("Data Quality", "Invalid Records").increment(1);
                    return;
                }

                String city = fields[0].trim();
                context.write(new Text(city), new Text(pm25 + ",1"));

            } catch (NumberFormatException e) {
                context.getCounter("Data Quality", "Invalid Records").increment(1);
            }
        }
    }

    public static class PMReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text city, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double total = 0;
            int count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                total += Double.parseDouble(parts[0]);
                count += Integer.parseInt(parts[1]);
            }

            if (count > 0) {
                context.write(city, new DoubleWritable(total / count));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: AveragePM25 <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average PM2.5 by City");

        job.setJarByClass(AveragePM25.class);
        job.setMapperClass(PMMapper.class);
        job.setReducerClass(PMReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
