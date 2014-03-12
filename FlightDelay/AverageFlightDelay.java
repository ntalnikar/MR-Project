import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AverageFlightDelay {

	// Global Counter
	public static enum DELAY_COUNTER {
		DELAY_SUM, DELAY_RECORDS
	};

	// Custom Key class
	public static class CustomKey implements WritableComparable<CustomKey> {

		// fields required for key
		private Text leg;
		private Text date;
		private Text intermediate;

		//constructors
		public CustomKey() {
			set(new Text(), new Text(), new Text());
		}

		public CustomKey(String leg, String date, String intermediate) {
			set(new Text(leg), new Text(date), new Text(intermediate));
		}

		public CustomKey(Text leg, Text date, Text intermediate) {
			set(leg, date, intermediate);
		}

		// get and set methods
		public void set(Text leg, Text date, Text intermediate) {
			this.leg = leg;
			this.date = date;
			this.intermediate = intermediate;
		}

		public Text getLeg() {
			return leg;
		}

		public Text getDate() {
			return date;
		}

		public Text getIntermediate() {
			return intermediate;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			leg.write(out);
			date.write(out);
			intermediate.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			leg.readFields(in);
			date.readFields(in);
			intermediate.readFields(in);
		}

		// override compareTo method
		@Override
		public int compareTo(CustomKey temp) {
			int cmp = leg.compareTo(temp.leg);
			if (cmp != 0) {
				return cmp;
			}
			cmp = date.compareTo(temp.date);
			if (cmp != 0)
				return cmp;
			return intermediate.compareTo(temp.intermediate);
		}
	}

	// Custom Value class
	public static class CustomValue implements WritableComparable<CustomValue> {

		// fields required for value
		private Text leg;
		private Text delay;
		private Text time;

		// constructors
		public CustomValue() {
			set(new Text(), new Text(), new Text());
		}

		public CustomValue(String leg, String delay, String time) {
			set(new Text(leg), new Text(delay), new Text(time));
		}

		public CustomValue(Text leg, Text delay, Text time) {
			set(leg, delay, time);
		}

		// get and set methods
		public void set(Text leg, Text delay, Text time) {
			this.leg = leg;
			this.delay = delay;
			this.time = time;
		}

		public Text getLeg() {
			return leg;
		}

		public Text getDelay() {
			return delay;
		}

		public Text getTime() {
			return time;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			leg.write(out);
			delay.write(out);
			time.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			leg.readFields(in);
			delay.readFields(in);
			time.readFields(in);
		}

		// override compareTo method
		@Override
		public int compareTo(CustomValue temp) {
			int cmp = leg.compareTo(temp.leg);
			if (cmp != 0) {
				return cmp;
			}
			cmp = delay.compareTo(temp.delay);
			if (cmp != 0)
				return cmp;
			return time.compareTo(temp.time);
		}
	}

	// Mapper Class
	public static class FlightMapper extends
			Mapper<Object, Text, CustomKey, CustomValue> {

		private CustomKey customKey = new CustomKey("", "", "");
		private CustomValue customValue = new CustomValue("", "", "");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser csv = new CSVParser();

			// each record from CSV file
			String[] str = csv.parseLine(value.toString());
			
			String month = str[2];
			String year = str[0];
			String cancelled = str[41];
			String diverted = str[43];
			String source = str[11];
			String dest = str[17];
			String arrivalTime = str[35];
			String departureTime = str[24];
			
			// Filter according to dates
			if ((Integer.parseInt(month)>=6 && year.equals("2007"))
					|| (Integer.parseInt(month)<=5 && year.equals("2008"))) {
				// Filter if not cancelled and diverted
				if (cancelled.equals("0.00") && diverted.equals("0.00")) {
					// Filter for F1 leg
					if (source.equals("ORD") && !dest.equals("JFK")) {
						customKey = new CustomKey("F1", str[5], dest);
						customValue = new CustomValue("F1", str[37],
								arrivalTime);
						// emit from map
						context.write(customKey, customValue);
					}

					// Filter for F2 leg
					if (dest.equals("JFK") && !source.equals("ORD")) {
						customKey = new CustomKey("F2", str[5], source);
						customValue = new CustomValue("F2", str[37],
								departureTime);
						// emit from map
						context.write(customKey, customValue);
					}
				}
			}
		}
	}

	// Reducer class
	public static class FlightDelayReducer extends
			Reducer<CustomKey, CustomValue, Text, IntWritable> {
	
		public void reduce(CustomKey key, Iterable<CustomValue> values,
				Context context) throws IOException, InterruptedException {
			
			ArrayList<CustomValue> F1flights = new ArrayList<CustomValue>();
			ArrayList<CustomValue> F2flights = new ArrayList<CustomValue>();

			// copy into 2 list based on F1 or F2
			for (CustomValue v : values) {
				if (v.getLeg().toString().equals("F1"))
					F1flights.add(new CustomValue(v.getLeg().toString(), v
							.getDelay().toString(), v.getTime().toString()));
				else
					F2flights.add(new CustomValue(v.getLeg().toString(), v
							.getDelay().toString(), v.getTime().toString()));
			}			
			
			// Do the JOIN by looping through F2 for every flight in F1 list
			for (int i = 0; i < F1flights.size(); i++) {				
				for (int j = 0; j < F2flights.size(); j++) {
					int delay1 = (int) Float.parseFloat(F1flights.get(i).getDelay()
							.toString());
					int arrival = (int) Float.parseFloat(F1flights.get(i).getTime()
							.toString());
					int temp = 0;
					int delay2 = (int) Float.parseFloat(F2flights.get(j)
							.getDelay().toString());
					int depart = (int) Float.parseFloat(F2flights.get(j)
							.getTime().toString());
					if (depart > arrival) {
						temp = delay1 + delay2;						
						// increment global counters
						context.getCounter(DELAY_COUNTER.DELAY_RECORDS).increment(1);
						context.getCounter(DELAY_COUNTER.DELAY_SUM).increment(temp);
					}
				}
			}			
		}
	}

	// Custom Partitioner
	public static class CustomPartitioner extends
			Partitioner<CustomKey, CustomValue> {
		@Override
		// Divide into partitions based on date
		public int getPartition(CustomKey key, CustomValue value,
				int numReduceTasks) {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
			Date date = null;
			try {
				date = formatter.parse(key.getDate().toString());
			} catch (ParseException e) {				
				e.printStackTrace();
			}
			return Math.abs(date.getDate() * 127) % numReduceTasks;
		}

	}

	// Key Comparator class
	public static class CustomKeyComparator extends WritableComparator {

		protected CustomKeyComparator() {
			super(CustomKey.class, true);
		}

		// compare keys based on date first, then intermediate airport
		public int compare(WritableComparable w1, WritableComparable w2) {
			CustomKey key1 = (CustomKey) w1;
			CustomKey key2 = (CustomKey) w2;
			int cmp = key1.getDate().compareTo(key2.getDate());
			if (cmp != 0)
				return cmp;
			return key1.getIntermediate().compareTo(key2.getIntermediate());
		}

	}

	// Group Comparator class
	// Controls which keys are grouped into a single call of the reduce function
	public static class CustomGroupComparator extends WritableComparator {
		protected CustomGroupComparator() {
			super(CustomKey.class, true);
		}

		// group according to date first, then intermediate airport
		public int compare(WritableComparable w1, WritableComparable w2) {
			CustomKey key1 = (CustomKey) w1;
			CustomKey key2 = (CustomKey) w2;
			int cmp = key1.getDate().compareTo(key2.getDate());
			if (cmp != 0)
				return cmp;
			return key1.getIntermediate().compareTo(key2.getIntermediate());
		}
	}

	// Main Program
	public static void main(String[] args) throws Exception {			
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: flightdelay <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(AverageFlightDelay.class);
		job.setMapperClass(FlightMapper.class);		

		// Set partitioner, key comparator and group comparator
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(CustomKeyComparator.class);
		job.setGroupingComparatorClass(CustomGroupComparator.class);

		job.setReducerClass(FlightDelayReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(CustomValue.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean result = job.waitForCompletion(true);
	
		// get counters and display average
		Counters counters = job.getCounters();
		Counter c1 = (Counter) counters.findCounter(DELAY_COUNTER.DELAY_RECORDS);
		int delayRecords = (int) c1.getValue();
		Counter c2 = (Counter) counters.findCounter(DELAY_COUNTER.DELAY_SUM);
		float delaySum = c2.getValue();
		Log log = LogFactory.getLog(AverageFlightDelay.class);
		log.info("Average Delay: "+(float)(delaySum/delayRecords));
	}
}
