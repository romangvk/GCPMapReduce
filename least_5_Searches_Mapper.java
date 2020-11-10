import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class least_5_Searches_Mapper extends Mapper<Object, Text, Text, LongWritable> {

	private TreeMap<Long, String> tmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		tmap = new TreeMap<Long, String>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// input data format => search_text
		// no_of_searches (tab separated)
		// we split the input data
		String[] tokens = value.toString().split("\t");

		String search = tokens[0];
		long no_of_searches = Long.parseLong(tokens[1]);

		// insert data into treeMap,
		// we want least 5 searched searches
		// so we pass no_of_searches as key
		tmap.put(0-no_of_searches, search);

		// we remove the first key-value
		// if it's size increases 5
		if (tmap.size() > 5) {
			tmap.remove(tmap.firstKey());
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Long, String> entry : tmap.entrySet()) {

			long count = entry.getKey();
			String search = entry.getValue();

			context.write(new Text(search), new LongWritable(0-count));
		}
	}
}
