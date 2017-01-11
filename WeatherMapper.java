import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String year = line.substring(15, 19);
		String month = line.substring(19,21);
		String day = line.substring(21,23);
		String hour = line.substring(23,25);
		String min = line.substring(25,27);
		String date = year + "-" + month + "-"+ day + " "+ hour + ":" + min + ":00.00";

		String wind = line.substring(65, 69);
		if(Integer.parseInt(wind) == 9999){
			wind = "MISSING";		
		}
		wind += ",";

		String vis = line.substring(78, 84);
		if(Integer.parseInt(vis) == 999999){
			vis = "MISSING";
		}
		vis += ",";

		String temper = line.substring(87, 92);
		if(Integer.parseInt(temper) == 9999){
			temper = "MISSING";
		}
		temper += ",";

		String keyWord ="AW1";
		String aw1 = "";
		int i = line.indexOf(keyWord);
		if(i != -1 &&line.length() > (i+4) && Character.isDigit(line.charAt(i+3)) && Character.isDigit(line.charAt(i+4))){
			aw1 = line.substring(i+3,i+5);
		}else{
			aw1 = "MISSING";
		}
		aw1 +=",";
			
		String keyWord2 = "AU1";
		String au1 = "";
		int j = line.indexOf(keyWord2);
		if(j!= -1 && line.length() > j+10){
			au1 = line.substring(j+3,j+10);
			if(!isInteger(au1)){
				au1 = "MISSING";
			}
		}else{
			au1 = "MISSING";
		}
		
		
		String weather = wind + vis + temper + aw1 + au1;
		context.write(new Text(date),new Text(weather));
		
	}
	public static boolean isInteger(String str) {
		if (str == null) {
        		return false;
    		}
    		int length = str.length();
    		if (length == 0) {
        	return false;
   		}
    		int i = 0;
    		if (str.charAt(0) == '-') {
        		if (length == 1) {
            			return false;
        		}
        	i = 1;
    		}
    		for (; i < length; i++) {
        		char c = str.charAt(i);
        		if (c < '0' || c > '9') {
            			return false;
        		}
    		}
    		return true;
	}
}

