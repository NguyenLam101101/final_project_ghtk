package com.example.projectxxx;
import com.example.projectxxx.SparkStreaming.Streaming;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProjectxxxApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(ProjectxxxApplication.class, args);

        Streaming streaming = new Streaming();
        streaming.stream();

//        Controller controller = new Controller();
//        controller.sendRequest();

    }
}
