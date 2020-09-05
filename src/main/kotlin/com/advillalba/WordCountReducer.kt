package com.advillalba

import mu.KotlinLogging
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer : Reducer<Text, IntWritable, Text, IntWritable>() {
    private val logger = KotlinLogging.logger {}
    override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
        logger.info {
            """Argumentos introducidos en el reduce con clave ${key}"""
        }
        var sum = 0
        for (value in values) {
            sum += value.get()
        }
        logger.info { "Suma: ${sum}" }
        context.write(key, IntWritable(sum))
    }


}