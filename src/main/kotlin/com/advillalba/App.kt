package com.advillalba

import mu.KotlinLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class App {
    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmStatic
        fun main(args: Array<String>) {
            validateArguments(args)
            val job = configureJob(args)
            System.exit(if (job.waitForCompletion(true)) 0 else 1)
        }

        private fun validateArguments(args: Array<String>) {
            if (args.size != 2) {
                logger.error { "2 argumentos requeridos. Introducidos: ${args.size}" }
                throw IllegalArgumentException("Numero de argumentos incorrectos")
            }
        }

        private fun configureJob(args: Array<String>): Job {
            val job = Job.getInstance()
            job.jobName = "WordCount"
            job.setJarByClass(App::class.java)
            job.mapperClass = WordCountMapper::class.java
            job.reducerClass = WordCountReducer::class.java
            job.outputKeyClass = Text::class.java
            job.outputValueClass = IntWritable::class.java
            FileInputFormat.addInputPath(job, Path(args[0]))
            FileOutputFormat.setOutputPath(job, Path(args[1]))
            return job
        }
    }

}