import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper
extends Mapper<LongWritable, Text, Text, Text> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
String line = value.toString();
String[] attributes = line.split(",");
if (attributes[0] != "BEGIN_YEARMONTH") {
	
String string = new String(attributes[12]+","+formatDate(attributes[17]) + "," + formatDate(attributes[19]));
context.write(new Text(attributes[7]), new Text(string));
}
}

String formatDate(String dt) {
	String[] array = dt.split(" |-|:|/");
	for (int i = 1; i <= 3; i++) {
		if (array[i].length() == 1) {
			array[i] = "0" + array[i];
		}
	}
	String result = array[0] + "-" + array[1] + "-" +  array[2] + " " + array[3] + ":" + array[4] + ":00" ;
	return result;
}
}

