//package com.example.projectxxx.Kafka.Service;
//
//import com.aspose.cells.Workbook;
//import com.example.projectxxx.Kafka.Config.KafkaProducerConfig;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import java.io.*;
//import java.util.concurrent.TimeUnit;
//
//public class SendMessage {
//    KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
//    public File sourceData = new File("src/stockData/");
//
//    public void send() throws Exception{
//        File[] indexs = sourceData.listFiles();
//        int i = 0;
//        for (File file:indexs) {
//            if (file.getName().endsWith("xls")) {
//                Workbook workbook = new Workbook(file.getPath());
//                workbook.save(file.getParent() + "/data" + String.valueOf(i) + ".txt");
//                i++;
//            }
//            if (file.getName().endsWith("txt")) {
//                FileReader fileReader = new FileReader(file.getPath());
//                BufferedReader bufferedReader = new BufferedReader(fileReader);
//                String line = bufferedReader.readLine(), nextLine;
//                int lineIndex = 1;
//                while (line != null) {
//                    nextLine = bufferedReader.readLine();
//                    if (lineIndex>=17) {
//                        if (nextLine == null){
//                            break;
//                        }
//                        kafkaProducerConfig.kafkaTemplate().send("stock", line);
//                        if (lineIndex%5==0)
//                            TimeUnit.SECONDS.sleep(1);
//                    }
//                    line = nextLine;
//                    lineIndex++;
//                }
//            }
//        }
//    }
//}
