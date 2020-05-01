package io.palyvos.provenance.usecases.cars.cloud;

import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class CarCloudDataProvider {


    public static void main(String[] args) throws java.io.IOException, ParseException, InterruptedException {

        // command line option parsing --------------------------
        Options options = new Options();
        Option hostnameArg = new Option("h", "hostname", true, "hostname");
        options.addOption(hostnameArg);
        Option portArg = new Option("p", "port", true, "port");
        options.addOption(portArg);
        Option filepathArg = new Option("f", "filepath", true, "filepath");
        options.addOption(filepathArg);
        Option replayArg = new Option("r", "replay", true, "define replay speed, if none is given, replay at maximum speed");
        replayArg.setRequired(false);
        options.addOption(replayArg);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        cmd = parser.parse(options, args);

        // end command line option parsing -----------------------

        String hostname = cmd.getOptionValue("hostname");
        int port = Integer.parseInt(cmd.getOptionValue("port"));
        String filepath = cmd.getOptionValue("filepath");
        double replaySpeed = Double.parseDouble(cmd.getOptionValue("replay","-1"));
        boolean doReplay = true;
        if (replaySpeed <= 0.0)
            doReplay = false;


        System.out.println("--CarCloudDataProvider: Waiting for connection on " + hostname + ", " + port);

        long previousTimeStamp = 0;
        String line;


        try(
                ServerSocket serverSocket = new ServerSocket(port);
                Socket socket = serverSocket.accept();
                ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
        )
        {
            System.out.println("--CarCloudDataProvider: Connection accepted, beginning transmission.");
            try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
                while ((line = br.readLine()) != null) {

                    String[] tokens = line.split(",");

                    long timestamp = Long.parseLong(tokens[0]);
                    int carID = Integer.parseInt(tokens[1]);
                    double lat = Double.parseDouble(tokens[2]);
                    double lon = Double.parseDouble(tokens[3]);

                    // artificial delay
                    if ((previousTimeStamp != 0) & (doReplay)) {
                        Thread.sleep((long) ((timestamp - previousTimeStamp) * 1000 / (replaySpeed)));
                    }

//                    if (carID < 100) {

                        // write to socket
                        objectOutput.writeUnshared(Tuple4.of(timestamp, carID, lat, lon));
                        objectOutput.reset();
//                    }

                    previousTimeStamp = timestamp;
                }
            }

        }
        System.out.println("--Transmission finished.");

    }

}
