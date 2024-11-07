import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Hadoop MapReduce program that finds the average delay in
 * two-leg flights from ORD -> JFK airports. Amongst other filters,
 * the data is filtered to only include flights between June 2007
 * & May 2008.
 */
public class PLAIN {
    // Origin and destination airports
    private static final String f1Origin = "ORD";
    private static final String f2Destination = "JFK";

    // Indexes of desired data
    private static final int yearIndex = 0;
    private static final int monthIndex = 2;
    private static final int flightDateIndex = 5;
    private static final int originIndex = 11;
    private static final int destinationIndex = 17;
    private static final int departureTimeIndex = 24;
    private static final int arrivalTimeIndex = 35;
    private static final int arrDelayMinutesIndex = 37;
    private static final int cancelledIndex = 41;
    private static final int divertedIndex = 43;
    private static final int minYear = 2007;
    private static final int minMonth = 6;
    private static final int maxYear = 2008;
    private static final int maxMonth = 5;

    /**
     * The main method that sets up the Hadoop MapReduce job configuration.
     * It specifies the Mapper, Reducer, Partitioner, and other job parameters.
     *
     * @param args Command line arguments: input and output file paths.
     * @throws Exception If an error occurs during job configuration or execution.
     */
    public static void main(String[] args) throws Exception {
        // Declare CSVParser.jar path
        Path csvParserPath = new Path("s3://a3b/opencsv.jar");

        // Jop 1 Configurations
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Flight delay filter and pair");

        job.addFileToClassPath(csvParserPath);
        job.setJarByClass(PLAIN.class);
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        // Job 1 Paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path betweenOutput = new Path(otherArgs[1] + "/middle");
        FileOutputFormat.setOutputPath(job, betweenOutput);

        // Wait for job completion
        boolean completed = job.waitForCompletion(true);

        if (completed) {
            //Job 2 Configuration
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Flight delay tally results");
            job2.addFileToClassPath(csvParserPath);

            job2.setJarByClass(PLAIN.class);
            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);

            // Job 2 Paths
            FileInputFormat.addInputPath(job2, betweenOutput);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/final"));

            // Wait for job completion & exit
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }

    /**
     * Mapper class for the first MapReduce job.
     * Filters and transforms flight data from a CSV file.
     * Emits each relevant flight leg as key-value pairs based on flight origin and destination.
     */
    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {
        private final Text emitKey = new Text();
        private final Text emitValue = new Text();
        private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();

        /**
         * Method to convert a time in HHMM to minutes.
         *
         * @param time A string representation of the time.
         * @return The time converted to minutes.
         */
        private static int convertToMinutes(String time) {
            int hour = Integer.parseInt(time.substring(0, 2));
            int minute = Integer.parseInt(time.substring(2));
            return hour * 60 + minute;
        }

        /**
         * The map method processes each line of input, filters by specified conditions,
         * and emits flight leg details for valid flights.
         *
         * @param key     The input key (usually the byte offset).
         * @param value   The input value (a line of text).
         * @param context The context for writing output key-value pairs.
         * @throws IOException If an I/O error occurs.
         */
        public void map(Object key, Text value, Context context) throws IOException {
            String[] fields = csvParser.parseLine(value.toString());

            try {
                int year = Integer.parseInt(fields[yearIndex]);
                int month = Integer.parseInt(fields[monthIndex]);

                // Filter by date range (June 2007 - May 2008)
                if (!((year == minYear && month >= minMonth) || (year == maxYear && month <= maxMonth))) {
                    return;
                }

                boolean cancelled = fields[cancelledIndex].equals("1");
                boolean diverted = fields[divertedIndex].equals("1");

                // Filter out cancelled or diverted flights
                if (cancelled || diverted) {
                    return;
                }

                String origin = fields[originIndex];
                String destination = fields[destinationIndex];

                // Filter flights that do not have desired origin or destination
                if (!origin.equals(f1Origin) && !destination.equals(f2Destination)) {
                    return;
                }

                // Filter 1-legged flights
                if (origin.equals(f1Origin) && destination.equals(f2Destination)) {
                    return;
                }

                String flightDate = fields[flightDateIndex];
                int arrTime = convertToMinutes(fields[arrivalTimeIndex]);
                int depTime = convertToMinutes(fields[departureTimeIndex]);
                double arrDelayMinutes = Double.parseDouble(fields[arrDelayMinutesIndex]);

                if (origin.equals(f1Origin)) {
                    emitKey.set(flightDate + "|" + destination);
                    emitValue.set("F1|" + arrTime + "|" + arrDelayMinutes);

                    // Emit ORD -> X flights as "F1" leg
                    context.write(emitKey, emitValue);
                } else {
                    emitKey.set(flightDate + "|" + origin);
                    emitValue.set("F2|" + depTime + "|" + arrDelayMinutes);

                    // Emit X -> JFK flights as "F2" leg
                    context.write(emitKey, emitValue);
                }
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Reducer class for the first MapReduce job.
     * This reducer is responsible for pairing two-leg flights (ORD -> X and X -> JFK),
     * where flights from ORD to an intermediary airport (F1) are paired with flights
     * from that airport to JFK (F2). It calculates the total delay and counts the
     * number of valid paired routes for each date.
     */
    public static class FlightReducer extends Reducer<Text, Text, Text, Text> {
        private final Text delay = new Text();
        private final Text count = new Text();

        /**
         * The reduce method processes each date and airport pair, grouping flights from ORD to X (F1)
         * and from X to JFK (F2). It finds valid flight pairs with an F1 arrival followed by an F2 departure.
         * For each valid pair, it calculates the total combined delay and increments the flight count.
         *
         * @param key     The key representing the date and intermediary airport (e.g., "2007-06-15|ATL").
         * @param values  The iterable list of Text values representing flight leg data (arrival or departure times and delay).
         * @param context The context for writing the output key-value pairs (total delay and count).
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the reducer is interrupted.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<Double>> f1Flights = new HashMap<>();
            Map<String, List<Double>> f2Flights = new HashMap<>();

            // Parse and add flights into f1 and f2 maps based on leg
            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                String legType = parts[0];
                String time = parts[1];
                double delay = Double.parseDouble(parts[2]);

                if (legType.equals("F1")) {
                    f1Flights.put(time, addFlightLegToLocalStore(f1Flights.get(time), delay));
                } else if (legType.equals("F2")) {
                    f2Flights.put(time, addFlightLegToLocalStore(f2Flights.get(time), delay));
                }
            }

            double totalDelay = 0;
            int flightCount = 0;

            // Pair flights from ORD to X with flights from X to JFK based on time sequence
            for (String f1Time : f1Flights.keySet()) {
                int f1ArrivalMinutes = Integer.parseInt(f1Time);
                List<Double> f1 = f1Flights.get(f1Time);
                double f1Delay = f1.get(0);

                for (String f2Time : f2Flights.keySet()) {
                    int f2DepartureMinutes = Integer.parseInt(f2Time);
                    List<Double> f2 = f2Flights.get(f2Time);
                    double f2Delay = f2.get(0);

                    // Check if the F2 flight departs after F1 flight arrival
                    if (f2DepartureMinutes > f1ArrivalMinutes) {
                        totalDelay += f1Delay + f2Delay;
                        flightCount += (int) Math.max(f1.get(1), f2.get(1));
                    }
                }
            }

            if (flightCount > 0) {
                delay.set(totalDelay + ",");
                count.set(String.valueOf(flightCount));
                context.write(delay, count);
            }
        }

        /**
         * Adds delay information for each flight leg (ORD -> X or X -> JFK) to a local storage map.
         * Initializes the list if it does not exist, and updates delay and count if it does.
         *
         * @param flightData The current list storing delay and count for a given leg.
         * @param delay      The delay for the current flight.
         * @return An updated list with total delay and count for the specific leg.
         */
        private List<Double> addFlightLegToLocalStore(List<Double> flightData, double delay) {
            if (flightData == null) {
                flightData = new ArrayList<>();
                flightData.add(delay);
                flightData.add(1.0);
            } else {
                flightData.set(0, flightData.get(0) + delay);
                flightData.set(1, flightData.get(1) + 1);
            }

            return flightData;
        }
    }

    /**
     * Secondary Mapper class that aggregates total flight counts and delays.
     * Emits aggregated results for further reduction.
     */
    public static class SecondMapper extends Mapper<Object, Text, Text, Text> {
        private final Text emitKey = new Text("F1F2");
        private double totalDelay;
        private int totalFlights;

        /**
         * Setup method that initializes the local counters.
         *
         * @param context The context of the Mapper task.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the setup is interrupted.
         */
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.totalDelay = 0;
            this.totalFlights = 0;
        }

        /**
         * The map method processes and immediately emits each line of input.
         *
         * @param key     The input key (usually the byte offset).
         * @param value   The input value (flightDelay, flightCount).
         * @param context The context for writing output key-value pairs.
         * @throws IOException If an I/O error occurs.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] flightInfo = value.toString().split(",");
            this.totalDelay += Double.parseDouble(flightInfo[0].trim());
            this.totalFlights += Integer.parseInt(flightInfo[1].trim());
        }

        /**
         * Cleanup method that emits the locally aggregated word counts
         * from the map task's tally counter.
         *
         * @param context The context for writing output key-value pairs.
         * @throws IOException          If an I/O error occurs.
         * @throws InterruptedException If the cleanup is interrupted.
         */
        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            context.write(this.emitKey, new Text(this.totalDelay + "," + this.totalFlights));
        }
    }

    /**
     * Secondary Reducer class for summing the flight counts and total flight delays.
     * It receives a flight count and delay from each mapper then aggregates
     * each respectively. Lastly, it emits the average delay (total delay / flight count).
     */
    public static class SecondReducer extends Reducer<Text, Text, Text, Text> {
        private final Text finalOutput = new Text();
        private double totalDelay = 0;
        private int totalFlights = 0;

        /**
         * The reduce method finds the average flight delay by summing
         * the flight counts and flight delays.
         *
         * @param key     The word being reduced.
         * @param values  The flight counts and delays (flightDelay, flightCount).
         * @param context The context for writing the final output.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] flightInfo = value.toString().split(",");
                this.totalDelay += Double.parseDouble(flightInfo[0]);
                this.totalFlights += Integer.parseInt(flightInfo[1]);
            }

            double avgDelay = this.totalDelay / this.totalFlights;
            this.finalOutput.set("Total Average Delay: " + avgDelay + "--- Total Flights: " + this.totalFlights + "--- Total Delay: " + this.totalDelay);
            context.write(new Text("Output: \n"), this.finalOutput);
        }
    }
}