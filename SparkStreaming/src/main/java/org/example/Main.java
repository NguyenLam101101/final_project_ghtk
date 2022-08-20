package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.api.java.function.VoidFunction2;

import java.nio.charset.StandardCharsets;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.to_date;

public class Main {
    private static String bootstrap_sever = "localhost:9092";
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("projectxxx")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        spark.udf().register("deserialize", (byte[] value) -> {
            String strValue = new String(value, StandardCharsets.UTF_8);
            return strValue;
        }, DataTypes.StringType);

        Dataset<Row> dts = spark
                .readStream()
                .format("kafka")
                .option("startingOffsets","latest")
                .option("kafka.bootstrap.servers", bootstrap_sever)
                .option("subscribe", "stock")
                .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .load();

        Dataset<Row> valueDts = dts.selectExpr("deserialize(value) as value");
        Dataset<Row> splitDts = valueDts.select(
                to_date(split(valueDts.col("value"),",").getItem(1), "yyyy-MM-dd HH:mm:ss").as("date"),
                split(valueDts.col("value"),",").getItem(2).as("index"),
                split(valueDts.col("value"),",").getItem(3).cast(DataTypes.FloatType).as("reference_price"),
                split(valueDts.col("value"),",").getItem(4).cast(DataTypes.FloatType).as("opening_price"),
                split(valueDts.col("value"),",").getItem(5).cast(DataTypes.FloatType).as("closing_price"),
                split(valueDts.col("value"),",").getItem(6).cast(DataTypes.FloatType).as("max_price"),
                split(valueDts.col("value"),",").getItem(7).cast(DataTypes.FloatType).as("min_price"),
                split(valueDts.col("value"),",").getItem(8).cast(DataTypes.FloatType).as("average_price"),
                split(valueDts.col("value"),",").getItem(9).cast(DataTypes.FloatType).as("+-_variation"),
                split(valueDts.col("value"),",").getItem(10).cast(DataTypes.FloatType).as("%_variation"),
                split(valueDts.col("value"),",").getItem(11).cast(DataTypes.LongType).as("matching_volume"),
                split(valueDts.col("value"),",").getItem(12).cast(DataTypes.LongType).as("matching_value"),
                split(valueDts.col("value"),",").getItem(13).cast(DataTypes.LongType).as("agreement_volume"),
                split(valueDts.col("value"),",").getItem(14).cast(DataTypes.LongType).as("agreement_value"),
                split(valueDts.col("value"),",").getItem(15).cast(DataTypes.LongType).as("total_volume"),
                split(valueDts.col("value"),",").getItem(16).cast(DataTypes.LongType).as("total_value"),
                split(valueDts.col("value"),",").getItem(17).cast(DataTypes.LongType).as("capitalization"));

        VoidFunction2<Dataset<Row>, Long> saveFunction = new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                rowDataset.write()
                        .mode("append")
                        .format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/stock")
                        .option("dbtable","history_price")
                        .option("user","root")
                        .option("password","NguyenLam2001")
                        .option("driver","com.mysql.cj.jdbc.Driver")
                        .save();
            }
        };

        StreamingQuery query = splitDts.writeStream()
                .outputMode("append")
//                .format("parquet")
//                .option("path","/user/lamnv155/output/")
                .foreachBatch(saveFunction)
                .option("checkpointLocation","checkpoint/")
                .trigger(Trigger.ProcessingTime(1000))
                .start();

        query.awaitTermination();
    }
}