package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by andresmacedo on 6/15/16.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


        String line = value.toString();
        for (String word : line.split(" ")){
            if(word.length()>0){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

}
