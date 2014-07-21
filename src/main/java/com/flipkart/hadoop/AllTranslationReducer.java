package com.flipkart.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sudhanshu.gupta on 09/02/14.
 */
public class AllTranslationReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String translation = "";
        for(Text val : values) {
            translation += "|" + val.toString();
        }
        result.set(translation);
        context.write(key, result);
        return;
    }
}
