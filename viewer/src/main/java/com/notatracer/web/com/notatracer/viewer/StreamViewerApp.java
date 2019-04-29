package com.notatracer.web.com.notatracer.viewer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class StreamViewerApp implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamViewerApp.class);

    public static void main(String[] args) {

        LOGGER.trace("StreamViewerApp::main");
        SpringApplication.run(StreamViewerApp.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOGGER.info("Application started w/ cmd-line args: {}", Arrays.toString(args.getSourceArgs()));
    }
}
