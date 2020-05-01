package io.palyvos.provenance.usecases.cars.local;

import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CarLocalDataProviderDebug {

    static long get_ts(File file) {
        String[] fileNameWithoutEndingSplitByUnderscore =  file.getName().replace("objects_","").split("\\.")[0].split("-");
        return Long.parseLong(fileNameWithoutEndingSplitByUnderscore[fileNameWithoutEndingSplitByUnderscore.length-1]);
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {

        // command line option parsing --------------------------
        Options options = new Options();
        Option hostnameArg = new Option("h", "hostname", true, "hostname");
        options.addOption(hostnameArg);
        Option portArg = new Option("p", "port", true, "port");
        options.addOption(portArg);
        Option folderpathArg = new Option("f", "folderpath", true, "port");
        options.addOption(folderpathArg);
        Option skipArg = new Option("s", "skipframes", true, "send only every n-th frame");
        options.addOption(skipArg);
        Option replayArg = new Option("r", "replay", true, "define replay speed, if none is given, replay at maximum speed");
        replayArg.setRequired(false);
        options.addOption(replayArg);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        cmd = parser.parse(options, args);

        // end command line option parsing -----------------------

        String hostname = cmd.getOptionValue("hostname");
        int port = Integer.parseInt(cmd.getOptionValue("port"));
        String folderpath = cmd.getOptionValue("folderpath");
        double replaySpeed = Double.parseDouble(cmd.getOptionValue("replay","-1"));
        boolean doReplay = true;
        if (replaySpeed <= 0.0)
            doReplay = false;
        int skipFrames = Integer.parseInt(cmd.getOptionValue("skipframes", "1"));


        List<Tuple4<Long, String, File, File>> timestampList= new ArrayList();

        File folder = new File(folderpath);
        File[] listOfFiles = folder.listFiles();


        File orderedFilelist = new File(folderpath + "/" + "orderedFilelist");

        boolean listExists = orderedFilelist.exists();

        List<Tuple7<Long, File, File, File, File, File, File>> combinedTimestampList = null;


        // we now need to load all filenames, match image/lidar file with its annotation file and sort everythoing
        // this is costly, so let's save the resulting ordered list. Before doing this, let's check if we already have
        // such a list (in the location given by 'folderpath'), and if we have, let's load that.
        // Then, we do not need to do this step every time we run the program. Nice, isn't it?
        if (listExists) {
            System.out.println("--Ordered filelist exists already, loading it now");

            try {
                FileInputStream fileIn = new FileInputStream(folderpath + "/" + "orderedFilelist");
                ObjectInputStream objectIn = new ObjectInputStream(fileIn);
                combinedTimestampList = (List<Tuple7<Long, File, File, File, File, File, File>>) objectIn.readObject();
                System.out.println("--Load succesful");
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        } else {

            System.out.println("--No existing ordered filelist found, creating it now");

            System.out.println("--Reading files into list and sorting by timestamp");

            for (File file : listOfFiles) {
                if (file.getName().contains("txt")) {
                    String cameraType;
                    cameraType = file.getName().replace("objects_","").split("-")[0]; //this is lidar, or ring_front_center, or ...
                    timestampList.add(new Tuple4<>(get_ts(file), cameraType, file, file));
                }
            }


            // sort the list of timestamps and files by timestamps
            timestampList.sort(Comparator.comparing(t0 -> t0.f0));

            combinedTimestampList = new ArrayList();

            for (int i = 0; i < timestampList.size(); i += 3) {
                System.out.println(timestampList.get(i));
                Tuple4<Long, String, File, File> elem_1 = timestampList.get(i);
                Tuple4<Long, String, File, File> elem_2 = timestampList.get(i + 1);
                Tuple4<Long, String, File, File> elem_3 = timestampList.get(i + 2);

                Tuple4<Long, String, File, File> elem1 = new Tuple4<>();
                Tuple4<Long, String, File, File> elem2 = new Tuple4<>();
                Tuple4<Long, String, File, File> elem3 = new Tuple4<>();

                if (elem_1.f1.equals("lidar")) {
                    elem1 = elem_1;
                } else if (elem_1.f1.contains("left")) {
                    elem2 = elem_1;
                } else {
                    elem3 = elem_1;
                }
                if (elem_2.f1.equals("lidar")) {
                    elem1 = elem_2;
                } else if (elem_2.f1.contains("left")) {
                    elem2 = elem_2;
                } else {
                    elem3 = elem_2;
                }
                if (elem_3.f1.equals("lidar")) {
                    elem1 = elem_3;
                } else if (elem_3.f1.contains("left")) {
                    elem2 = elem_3;
                } else {
                    elem3 = elem_3;
                }

                combinedTimestampList.add(Tuple7.of(elem1.f0, elem1.f2, elem1.f3, elem2.f2, elem2.f3, elem3.f2, elem3.f3));
            }

            System.out.printf("--Writing ordered filelist to disk for future use");

            try {

                FileOutputStream fileOut = new FileOutputStream(folderpath + "/" + "orderedFilelist");
                ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
                objectOut.writeObject(combinedTimestampList);
                objectOut.close();
                System.out.println("--Ordered filelist written to disk");

            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }




        System.out.println("--Waiting for connection on " + hostname + ", " + port);

        int counter = 0;
        long previousTimeStamp = 0;
        // emit LidarImageContainer objects
        try(
                ServerSocket serverSocket = new ServerSocket(port);
                Socket socket = serverSocket.accept();
                ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
        )
        {
            System.out.println("--Connection accepted, beginning transmission.");
            int i = 0;
            long j = 0;
            long maxIteration = 1000000000;
            int lengthList = combinedTimestampList.size();
            long highestTSinList = combinedTimestampList.get(lengthList-1).f0;
            long lowestTSinList = combinedTimestampList.get(0).f0;


            while (j < maxIteration) {
                Tuple7 timestampFilesAnnotations = combinedTimestampList.get(i);
//            for (Tuple7 timestampFilesAnnotations : combinedTimestampList) {
                long timestamp = (long) timestampFilesAnnotations.f0
                        + j / lengthList * (highestTSinList - lowestTSinList + 1);
                if ((previousTimeStamp != 0) & (doReplay)) {
                    Thread.sleep((long) ((timestamp - previousTimeStamp) / (1000000 * replaySpeed)));
                }
                LidarImageContainer container1 = new LidarImageContainer("lidar", timestamp / 1000000);
//                container1.readLiDAR((File) timestampFilesAnnotations.f1);
                container1.createAnnotations((File) timestampFilesAnnotations.f2);
                LidarImageContainer container2= new LidarImageContainer("ring_front_left", timestamp / 1000000);
//                container2.readImage((File) timestampFilesAnnotations.f3);
                container2.createAnnotations((File) timestampFilesAnnotations.f4);
                LidarImageContainer container3= new LidarImageContainer("ring_front_right", timestamp / 1000000);
//                container3.readImage((File) timestampFilesAnnotations.f5);
                container3.createAnnotations((File) timestampFilesAnnotations.f6);

                if (counter == 0) {
                    objectOutput.writeObject(Tuple3.of(container1, container2, container3));
                    objectOutput.reset();
                }

                counter = (counter + 1) % skipFrames;

                previousTimeStamp = timestamp;
                i++; i = i % lengthList;
                j++;
            }
        }
        System.out.println("--Transmission finished.");

    }

}
