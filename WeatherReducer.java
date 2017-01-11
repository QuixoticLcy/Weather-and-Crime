import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			if(value.toString().equals("MISSING,MISSING,MISSING,MISSING,MISSING")) continue;
			context.write(new Text(key), new Text(value.toString()));
			break;
		}


	}
}
