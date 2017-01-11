import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * 10514462,HZ2s56372,01/01/2015 12:00:00 AM,073XX S EXCHANGE AVE,0281,CRIM SEXUAL ASSAULT,
	 */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line = value.toString();
	  String crime[]=line.split(",");
	  String time=crime[2];
	  String type=crime[5];
	  String id=crime[0];
	  SimpleDateFormat displayFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SS");
      SimpleDateFormat parseFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
      Date date;
	try {
		date = parseFormat.parse(time);
	    String tempt = displayFormat.format(date);
	    String newtime=tempt.replace("/", "-");
		String description=","+newtime+","+type;
		context.write(new Text(id), new Text(description));	
	} catch (ParseException e) {
		
		e.printStackTrace();
	}
     	 
	   }
  }
 

