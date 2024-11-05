import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


/**
 * A Hadoop MapReduce program that counts words starting with
 * the letters 'm', 'n', 'o', 'p', 'q'. This version of the
 * program disables the combiner.
 */
public class PLAIN {

    private static final String f1Origin = "ORD";
    private static final String f2Destination = "JFK";

    /**
     * Map of characters 'm', 'n', 'o', 'p', and 'q'.
     * Used to partition words into different reducers and
     * to filter words based on their first letter.
     */
    private static final Map<Character, Integer> letters = new HashMap<Character, Integer>() {{
        put('m', 0);
        put('n', 1);
        put('o', 2);
        put('p', 3);
        put('q', 4);
    }};

    /**
     * The main method that sets up the Hadoop MapReduce job configuration.
     * It specifies the Mapper, Reducer, Partitioner, and other job parameters.
     *
     * @param args Command line arguments: input and output file paths.
     * @throws Exception If an error occurs during job configuration or execution.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "plain");
        job.setJarByClass(PLAIN.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class); // Combiner is disabled
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setPartitionerClass(WordPartitioner.class);
//        job.setNumReduceTasks(letters.size());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * A custom Partitioner class that partitions words by their
     * first character. Words starting with 'm' go to reducer 0,
     * 'n' to reducer 1, and so on.
     */
    public static class WordPartitioner extends Partitioner<Text, IntWritable> {
        /**
         * Assigns a partition to each word based on its first character.
         *
         * @param key           The word to partition.
         * @param value         The count associated with the word.
         * @param numPartitions The total number of partitions (reducers).
         * @return The partition number for the word.
         */
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            char firstChar = key.toString().toLowerCase().charAt(0);
            return letters.get(firstChar);
        }
    }

    /**
     * The Mapper class for tokenizing lines of input into words,
     * emitting each word with a count of 1 if it starts with one
     * of the target letters ('m', 'n', 'o', 'p', 'q').
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text emitKey = new Text();
        private final Text emitValue = new Text();

        /**
         * The map method processes each line of input, tokenizes it,
         * and emits each word with a count of 1 if the word starts
         * with the letters 'm', 'n', 'o', 'p', or 'q'.
         *
         * @param key     The input key (usually the byte offset).
         * @param value   The input value (a line of text).
         * @param context The context for writing output key-value pairs.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the map task is interrupted.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            int year = Integer.parseInt(fields[1]);
            int month = Integer.parseInt(fields[3]);
            String flightDate = fields[6];
            String origin = fields[12];
            String destination = fields[18];
            int depTime = Integer.parseInt(fields[25]);
            int arrTime = Integer.parseInt(fields[36]);
            double arrDelayMinutes = Double.parseDouble(fields[38]);
            boolean cancelled = fields[42].equals("1");
            boolean diverted = fields[44].equals("1");

            // Filter by date range (June 2007 - May 2008)
            if (!((year == 2007 && month >= 6) || (year == 2008 && month <= 5))) {
                return;
            }

            // Filter origin and destinations
            if (!Objects.equals(origin, f1Origin) || !Objects.equals(destination, f2Destination)) {
                return;
            }

            // Filter and check it is not a 1-leg flight
            if (Objects.equals(origin, f1Origin) && Objects.equals(destination, f2Destination)) {
                return;
            }

            // Filter out cancelled and diverted flights
            if (cancelled || diverted) {
                return;
            }

            if (origin.equals(f1Origin)) {
                emitKey.set(flightDate + "|" + destination);
                emitValue.set("F1|" + origin + "|" + destination + "|" + arrTime + "|" + arrDelayMinutes);

                // Emit ORD -> X flights as "F1" leg
                context.write(emitKey, emitValue);
            } else {
                emitKey.set(flightDate + "|" + origin);
                emitValue.set("F2|" + origin + "|" + destination + "|" + depTime + "|" + arrDelayMinutes);

                // Emit X -> JFK flights as "F2" leg
                context.write(emitKey, emitValue);
            }
        }
    }

    /**
     * The Reducer class for summing the counts of words from the map tasks.
     * It receives the word and its count from each mapper and aggregates
     * the count across all mappers.
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private final IntWritable result = new IntWritable();

        /**
         * The reduce method aggregates the word counts by summing
         * the counts for each word.
         *
         * @param key     The word being reduced.
         * @param values  The counts of the word from each map task.
         * @param context The context for writing the final output.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the reduce task is interrupted.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> firstLegFlights = new ArrayList<>();
            List<String> secondLegFlights = new ArrayList<>();
            double totalDelay = 0;
            int flightCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                String legType = parts[0];

                if (legType.equals("F1")) {
                    firstLegFlights.add(value.toString());
                } else if (legType.equals("F2")) {
                    secondLegFlights.add(value.toString());
                }
            }

            for (String f1 : firstLegFlights) {
                String[] f1Parts = f1.split("\\|");
                int f1ArrTime = Integer.parseInt(f1Parts[3]);
                double f1Delay = Double.parseDouble(f1Parts[4]);

                for (String f2 : secondLegFlights) {
                    String[] f2Parts = f2.split("\\|");
                    int f2DepTime = Integer.parseInt(f2Parts[3]);
                    double f2Delay = Double.parseDouble(f2Parts[4]);

                    if (f2DepTime > f1ArrTime) {
                        totalDelay += f1Delay + f2Delay;
                        flightCount++;
                    }
                }
            }

            if (flightCount > 0) {
                double averageDelay = totalDelay / flightCount;
                context.write(new Text("Average Delay"), new Text(Double.toString(averageDelay)));
            }
        }
    }
}