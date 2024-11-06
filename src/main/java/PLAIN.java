import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import javafx.beans.binding.DoubleExpression;
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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;
import org.eclipse.jetty.http.QuotedCSVParser;


/**
 * A Hadoop MapReduce program that counts words starting with
 * the letters 'm', 'n', 'o', 'p', 'q'. This version of the
 * program disables the combiner.
 */
public class PLAIN {
    private static final String f1Origin = "ORD";
    private static final String f2Destination = "JFK";
    private static final int minYear = 2007;
    private static final int minMonth = 1;
    private static final int maxYear = 2008;
    private static final int maxMonth = 12;

    /**
     * The main method that sets up the Hadoop MapReduce job configuration.
     * It specifies the Mapper, Reducer, Partitioner, and other job parameters.
     *
     * @param args Command line arguments: input and output file paths.
     * @throws Exception If an error occurs during job configuration or execution.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "plain");
        job.setJarByClass(PLAIN.class);
        job.setMapperClass(FlightMapper.class);
//        job.setCombinerClass(DelayCombiner.class); // Combiner is disabled
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setPartitionerClass(WordPartitioner.class);
//        job.setNumReduceTasks(12);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The Mapper class for tokenizing lines of input into words,
     * emitting each word with a count of 1 if it starts with one
     * of the target letters ('m', 'n', 'o', 'p', 'q').
     */
    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {
        private final Text emitKey = new Text();
        private final Text emitValue = new Text();
        private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();

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
        public void map(Object key, Text value, Context context) throws IOException {
            String[] fields = csvParser.parseLine(value.toString());

            try {
                int year = Integer.parseInt(fields[0]);
                int month = Integer.parseInt(fields[2]);
                String flightDate = fields[5];
                String origin = fields[11];
                String destination = fields[17];
                String depTime = fields[24];
                String arrTime = fields[35];
                double arrDelayMinutes = Double.parseDouble(fields[37]);
                boolean cancelled = fields[41].equals("1");
                boolean diverted = fields[43].equals("1");

                // Filter by date range (June 2007 - May 2008)
                if (!((year == minYear && month >= minMonth) || (year == maxYear && month <= maxMonth))) {
                    return;
                }

                // Filter out cancelled or diverted flights
                if (cancelled || diverted) {
                    return;
                }

                // Filter flights that do not have desired orgin or destination
                if (!origin.equals(f1Origin) && !destination.equals(f2Destination)) {
                    return;
                }

                // Filter 1-legged flights
                if (origin.equals(f1Origin) && destination.equals(f2Destination)) {
                    return;
                }

                if (origin.equals(f1Origin)) {
                    emitKey.set(flightDate + "|" + destination);
                    emitValue.set("F1|" + arrTime + "|" + arrDelayMinutes);

                    // Emit ORD -> X flights as "F1" leg
                    context.write(emitKey, emitValue);
                } else if (destination.equals(f2Destination)) {
                    emitKey.set(flightDate + "|" + origin);
                    emitValue.set("F2|" + depTime + "|" + arrDelayMinutes);

                    // Emit X -> JFK flights as "F2" leg
                    context.write(emitKey, emitValue);
                }
            } catch (Exception e) {}
        }
    }

    /**
     * The Reducer class for summing the counts of words from the map tasks.
     * It receives the word and its count from each mapper and aggregates
     * the count across all mappers.
     */
    public static class FlightReducer extends Reducer<Text, Text, Text, Text> {
        private final IntWritable result = new IntWritable();
        private final Text name = new Text();
        private final Text delay = new Text();

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
            Map<String, List<Double>> f1Flights = new HashMap<>();
            Map<String, List<Double>> f2Flights = new HashMap<>();

            // Parse and separate flights into f1 and f2 based on prefix
            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                String legType = parts[0];
                String time = parts[1];
                double delay = Double.parseDouble(parts[2]);

                if (legType.equals("F1")) {
                    // Check if the key exists, if not, initialize it
                    List<Double> flightData = f1Flights.get(time);
                    if (flightData == null) {
                        flightData = new ArrayList<>();
                        flightData.add(delay);  // Accumulate delay
                        flightData.add(1.0);     // Flight count
                    } else {
                        flightData.set(0, flightData.get(0) + delay);  // Update delay
                        flightData.set(1, flightData.get(1) + 1);      // Increment flight count
                    }
                    f1Flights.put(time, flightData);
                } else if (legType.equals("F2")) {
                    // Check if the key exists, if not, initialize it
                    List<Double> flightData = f2Flights.get(time);
                    if (flightData == null) {
                        flightData = new ArrayList<>();
                        flightData.add(delay);  // Accumulate delay
                        flightData.add(1.0);     // Flight count
                    } else {
                        flightData.set(0, flightData.get(0) + delay);  // Update delay
                        flightData.set(1, flightData.get(1) + 1);      // Increment flight count
                    }
                    f2Flights.put(time, flightData);
                }
            }

            // Calculate delays for valid connections
            double totalDelay = 0;
            int flightCount = 0;

            for (String f1Time : f1Flights.keySet()) {
                int f1ArrivalMinutes = convertToMinutes(f1Time);
                List<Double> f1 = f1Flights.get(f1Time);
                double f1Delay = f1.get(0);

                for (String f2Time : f2Flights.keySet()) {
                    int f2DepartureMinutes = convertToMinutes(f2Time);
                    List<Double> f2 = f2Flights.get(f2Time);
                    double f2Delay = f2.get(0);
                    if (f2DepartureMinutes > f1ArrivalMinutes) {
                        totalDelay += f1Delay + f2Delay;
                        flightCount += Math.max(f1.get(1), f2.get(1));
                    }
                }
            }

            double averageDelay = 0.0;
            if (flightCount > 0) {
                averageDelay = totalDelay / flightCount;
            }

            name.set("Average Delay " + key);
            delay.set(averageDelay + "|" + flightCount);
            context.write(name, delay);
        }
    }

    private static int convertToMinutes(String time) {
        int hour = Integer.parseInt(time.substring(0, 2));
        int minute = Integer.parseInt(time.substring(2));
        return hour * 60 + minute;
    }
}