package com.advillalba

import mu.KotlinLogging
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import java.util.*

class WordCountMapper : Mapper<LongWritable, Text, Text, IntWritable>() {
    private val one = IntWritable(1)
    private var word = Text()
    private val logger = KotlinLogging.logger {}
    override fun map(key: LongWritable, lineValue: Text, context: Context) {
        logger.info {
            """Argumentos introducidos en el mapper: 
|                           Clave: ${key}
|                           Valor: ${lineValue}""".trimMargin()
        }
        val tokenizer = StringTokenizer(lineValue.toString())

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken())
            context.write(word, one)
        }
    }
}