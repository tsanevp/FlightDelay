//import com.opencsv.CSVParser;
//import com.opencsv.CSVParserBuilder;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//
//import static util.Constants.*;
//
///**
// * The Mapper class for tokenizing lines of input into words,
// * emitting each word with a count of 1 if it starts with one
// * of the target letters ('m', 'n', 'o', 'p', 'q').
// */
//public class FlightMapper extends Mapper<Object, Text, Text, Text> {
//    private final Text emitKey = new Text();
//    private final Text emitValue = new Text();
//    private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();
//
//    private static int convertToMinutes(String time) {
//        int hour = Integer.parseInt(time.substring(0, 2));
//        int minute = Integer.parseInt(time.substring(2));
//        return hour * 60 + minute;
//    }
//
//    /**
//     * The map method processes each line of input, tokenizes it,
//     * and emits each word with a count of 1 if the word starts
//     * with the letters 'm', 'n', 'o', 'p', or 'q'.
//     *
//     * @param key     The input key (usually the byte offset).
//     * @param value   The input value (a line of text).
//     * @param context The context for writing output key-value pairs.
//     * @throws IOException If an I/O error occurs.
//     */
//    public void map(Object key, Text value, Context context) throws IOException {
//        String[] fields = csvParser.parseLine(value.toString());
//
//        try {
//            int year = Integer.parseInt(fields[yearIndex]);
//            int month = Integer.parseInt(fields[monthIndex]);
//
//            // Filter by date range (June 2007 - May 2008)
//            if (!((year == minYear && month >= minMonth) || (year == maxYear && month <= maxMonth))) {
//                return;
//            }
//
//            boolean cancelled = fields[cancelledIndex].equals("1");
//            boolean diverted = fields[divertedIndex].equals("1");
//
//            // Filter out cancelled or diverted flights
//            if (cancelled || diverted) {
//                return;
//            }
//
//            String origin = fields[originIndex];
//            String destination = fields[destinationIndex];
//
//            // Filter flights that do not have desired origin or destination
//            if (!origin.equals(f1Origin) && !destination.equals(f2Destination)) {
//                return;
//            }
//
//            // Filter 1-legged flights
//            if (origin.equals(f1Origin) && destination.equals(f2Destination)) {
//                return;
//            }
//
//            String flightDate = fields[flightDateIndex];
//            int arrTime = convertToMinutes(fields[arrivalTimeIndex]);
//            int depTime = convertToMinutes(fields[departureTimeIndex]);
//            double arrDelayMinutes = Double.parseDouble(fields[arrDelayMinutesIndex]);
//
//            if (origin.equals(f1Origin)) {
//                emitKey.set(flightDate + "|" + destination);
//                emitValue.set("F1|" + arrTime + "|" + arrDelayMinutes);
//
//                // Emit ORD -> X flights as "F1" leg
//                context.write(emitKey, emitValue);
//            } else {
//                emitKey.set(flightDate + "|" + origin);
//                emitValue.set("F2|" + depTime + "|" + arrDelayMinutes);
//
//                // Emit X -> JFK flights as "F2" leg
//                context.write(emitKey, emitValue);
//            }
//        } catch (Exception ignored) {
//        }
//    }
//}