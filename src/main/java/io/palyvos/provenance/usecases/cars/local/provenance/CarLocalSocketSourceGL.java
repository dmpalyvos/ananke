package io.palyvos.provenance.usecases.cars.local.provenance;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.net.Socket;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CarLocalSocketSourceGL
        extends
    RichSourceFunction<CarLocalInputTupleGL> {
    private static final String NAME = "SOURCE";

    // Default delay between successive connection attempts
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;
    // Default connection timeout when connecting to the server socket (infinite)
    private static final int CONNECTION_TIMEOUT_TIME = 0;

    private final String hostname;
    private final int port;


    private final long maxNumRetries;
    private final long delayBetweenRetries;
    private final ExperimentSettings settings;

    private volatile boolean isRunning = true;
    private transient CountStat throughputStatistic;

    // --------------- constructor and methods --------------------------


    public CarLocalSocketSourceGL(String hostname, int port, long maxNumRetries,
        long delayBetweenRetries, ExperimentSettings settings) {
        this.hostname = hostname;
        this.port = port;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
        this.settings = settings;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.throughputStatistic =
            new CountStat(
                settings.throughputFile(NAME, getRuntimeContext().getIndexOfThisSubtask()),
                settings.autoFlush());
    }

    @Override
    public void run(SourceContext<CarLocalInputTupleGL> ctx) throws Exception {

        long attempt = 0;

        while (isRunning) {

            try {
                Socket socket = new Socket(hostname, port);
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

                while (isRunning) {
                    Tuple3<LidarImageContainer,LidarImageContainer,LidarImageContainer> receivedTuple;
                    receivedTuple = (Tuple3<LidarImageContainer,LidarImageContainer,LidarImageContainer>) input.readObject();
                    long timestamp = receivedTuple.f0.getTimestamp();
//                    int tupleID = Long.toString(timestamp).hashCode();
                    LidarImageContainer lidar = receivedTuple.f0;
                    LidarImageContainer left  = receivedTuple.f1;
                    LidarImageContainer right = receivedTuple.f2;
                    throughputStatistic.increase(1);
                    CarLocalInputTupleGL tuple = new CarLocalInputTupleGL(timestamp, lidar, left,
                        right);
                    tuple.initGenealog(GenealogTupleType.SOURCE);
                    ctx.collectWithTimestamp(tuple, timestamp);

                }

                if (this.isRunning) {
                    ++attempt;
                    if (this.maxNumRetries != -1L && attempt >= this.maxNumRetries) {
                        break;
                    }
                    Thread.sleep(delayBetweenRetries);
                }
            } catch (EOFException exception) {
                this.isRunning = false;
                throughputStatistic.close();
                return;
            }
        }
        throughputStatistic.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}


