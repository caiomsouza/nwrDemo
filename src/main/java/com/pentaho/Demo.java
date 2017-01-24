package com.pentaho;

import com.hi3project.vineyard.comm.stomp.gozirraws.Client;
import com.hi3project.vineyard.comm.stomp.gozirraws.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/* ***********************************************************************************************
 * Program connects to the Network Rail Data Feeds using the Stomp Client Listener.
 * The Listener is Thread-safe and opens separate connections per-call, per-topic.
 * *********************************************************************************************** */
public class Demo implements AutoCloseable {
    private static final String SERVER = "datafeeds.networkrail.co.uk";
    private static final int PORT = 61618;
    private static final String USERNAME = "venkatesh@zyobee.com";
    private static final String PASSWORD = "1qaZXsw2@";

    private static final Logger LOGGER = LoggerFactory.getLogger("Demo");

    private List<String> topics;
    private BufferedReader reader;

    private Client client;

    private boolean init0() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("topics");
        reader = new BufferedReader(new InputStreamReader(is));
        topics = reader.lines().collect(Collectors.toList());

        client = new Client(SERVER, PORT, USERNAME, PASSWORD);
        if (! client.isConnected()) {
            LOGGER.error("| Could not connect : " + client.toString());
            return false;
        }

        LOGGER.info("| Connected to : " + SERVER + ":" + PORT);
        LOGGER.info("| Subscribing to topics : " + topics);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                client.disconnect();
                LOGGER.info("| Client Disconnected ; System Shutting Down");
            }
        });

        return true;
    }

    private void go() throws Exception {
        Files.createDirectories(Paths.get("output"));

        for(String topic:topics)
            client.subscribe("/topic/" + topic,new TopicListener(topic));
    }

    class TopicListener implements Listener, AutoCloseable {
        final String topic;
        final Logger logger;

        BufferedWriter writer;

        TopicListener(final String topic) throws Exception {
            this.topic = topic;
            this.logger = LoggerFactory.getLogger(topic);
            this.writer = new BufferedWriter(new FileWriter("output/" + topic + "_" + LocalDateTime.now() + ".json"));
        }

        @Override
        public void message(Map header, String body) {
            try {
                writer.write(body);
                writer.newLine();
            } catch (IOException x) {
                logger.error("| Topic | " + topic + " | Message consumption failed : " + x.toString());
            }
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    /* ***********************************************************************************************
     * Starting Point
     * *********************************************************************************************** */
    public static void main(String... args) throws Exception {
        Demo demo = new Demo();
        if (demo.init0())
            demo.go();
    }
}