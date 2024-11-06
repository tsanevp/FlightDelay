//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * The Reducer class for summing the counts of words from the map tasks.
// * It receives the word and its count from each mapper and aggregates
// * the count across all mappers.
// */
//public class FlightReducer extends Reducer<Text, Text, Text, Text> {
//    private final Text delay = new Text();
//    private final Text count = new Text();
//
//    /**
//     * The reduce method aggregates the word counts by summing
//     * the counts for each word.
//     *
//     * @param key     The word being reduced.
//     * @param values  The counts of the word from each map task.
//     * @param context The context for writing the final output.
//     * @throws IOException          If an I/O error occurs.
//     * @throws InterruptedException If the reduce task is interrupted.
//     */
//    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        Map<String, List<Double>> f1Flights = new HashMap<>();
//        Map<String, List<Double>> f2Flights = new HashMap<>();
//
//        // Parse and add flights into f1 and f2 maps based on leg
//        for (Text value : values) {
//            String[] parts = value.toString().split("\\|");
//            String legType = parts[0];
//            String time = parts[1];
//            double delay = Double.parseDouble(parts[2]);
//
//            if (legType.equals("F1")) {
//                f1Flights.put(time, addFlightLegToLocalStore(f1Flights.get(time), delay));
//            } else if (legType.equals("F2")) {
//                f2Flights.put(time, addFlightLegToLocalStore(f2Flights.get(time), delay));
//            }
//        }
//
//        // Calculate delays for valid connections
//        double totalDelay = 0;
//        int flightCount = 0;
//
//        for (String f1Time : f1Flights.keySet()) {
//            int f1ArrivalMinutes = Integer.parseInt(f1Time);
//            List<Double> f1 = f1Flights.get(f1Time);
//            double f1Delay = f1.get(0);
//
//            for (String f2Time : f2Flights.keySet()) {
//                int f2DepartureMinutes = Integer.parseInt(f2Time);
//                List<Double> f2 = f2Flights.get(f2Time);
//                double f2Delay = f2.get(0);
//                if (f2DepartureMinutes > f1ArrivalMinutes) {
//                    totalDelay += f1Delay + f2Delay;
//                    flightCount += (int) Math.max(f1.get(1), f2.get(1));
//                }
//            }
//        }
//
//        if (flightCount > 0) {
//            delay.set(totalDelay + ",");
//            count.set(String.valueOf(flightCount));
//            context.write(delay, count);
//        }
//    }
//
//    // Check if the key exists, if not, initialize it
//    private List<Double> addFlightLegToLocalStore(List<Double> flightData, double delay) {
//        if (flightData == null) {
//            flightData = new ArrayList<>();
//            flightData.add(delay);
//            flightData.add(1.0);
//        } else {
//            flightData.set(0, flightData.get(0) + delay);
//            flightData.set(1, flightData.get(1) + 1);
//        }
//
//        return flightData;
//    }
//}