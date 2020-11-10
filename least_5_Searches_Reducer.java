import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class least_5_Searches_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {

	private TreeMap<Long, String> tmap2;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		tmap2 = new TreeMap<Long, String>();
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		// input data from mapper
		// key values
		// search [ count ]
		String search = key.toString();
		long count = 0;

		for (LongWritable val : values) {
			count = val.get();
		}

		// insert data into treeMap,
		// we want least 5 searched searches
		// so we pass count as key
		tmap2.put(0-count, search);

		// we remove the first key-value
		// if it's size increases 5
		if (tmap2.size() > 5) {
			tmap2.remove(tmap2.firstKey());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		for (Map.Entry<Long, String> entry : tmap2.entrySet()) {

			long count = entry.getKey();
			String search = entry.getValue();
			context.write(new LongWritable(0-count), new Text(search));
		}
	}
}
