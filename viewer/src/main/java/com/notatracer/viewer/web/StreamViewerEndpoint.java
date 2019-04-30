package com.notatracer.viewer.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StreamViewerEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamViewerEndpoint.class);

    @GetMapping("/viewer")
    public void fromBeginning() {
        LOGGER.info("Stream session from the beginning...");

        /*
        Connect to kafka topic
        Stream results from beginning.
         */

    }

    @GetMapping("/viewer/{startTimeHHMMSS}")
    public void fromBeginning(@PathVariable String startTimeHHMMSS) {
        LOGGER.info(String.format("Stream session starting from %s...", startTimeHHMMSS));
    }

}
