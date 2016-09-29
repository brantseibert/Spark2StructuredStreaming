import org.apache.spark.sql.types._

object StructuredStreamingPOC {


spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "AKIAITPOXF53PMYDU4NA")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "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")
// dbutils.fs.mount(s"s3a://<AWSID>:<AWS-KEY>@<BUCKET>", s"/mnt/bitcoin")


// Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
val inputPath = "/mnt/bitcoin/logs"
val jsonSchema  = spark.read.json("/mnt/bitcoin/logs").schema

import org.apache.spark.sql.functions._

val streamingInputDF = 
  spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)               // Set the schema of the JSON data
    .json(inputPath)


val streamingCountsDF = streamingInputDF
.selectExpr("op", "from_unixtime(x.time) as time",  "x.inputs[0].sequence as x_inputs_sequence", "x.inputs[0].prev_out.script as x_inputs_script", "x.hash as hash" , "x.tx_index as tx_index", "x.relayed_by as relayed_by")
.select($"op", window($"time", "1 minute"), $"x_inputs_sequence", $"x_inputs_script", $"hash", $"tx_index", $"relayed_by")

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming


spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small
spark.conf.set("spark.sql.streaming.checkpointLocation", "/mnt/bitcoin/checkpoint");
val query =
  streamingCountsDF
    .writeStream
    .format("parquet")
    .outputMode("append")  // complete = all the counts should be in the table
    .start("/mnt/bitcoin/parquet")



}
