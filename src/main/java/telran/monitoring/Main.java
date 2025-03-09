package telran.monitoring;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;

import telran.monitoring.api.SensorData;
import telran.monitoring.logging.Logger;
import telran.monitoring.logging.LoggerStandard;

public class Main {

    private static final int PORT = 5000;
    private static final int MAX_SIZE = 1500;
    private static final int WARNING_LOG_VALUE = 220;
    private static final int ERROR_LOG_VALUE = 230;
    private static final String DEFAULT_PULSE_VALUES_STREAM = "pulse_values";
    private static final String DEFAULT_STREAM_CLASS_NAME = "telran.monitoring.DynamoDbStreamSensorData";
    static Logger logger = new LoggerStandard("receiver");
    static Map<String, String> env = System.getenv();

    public static void main(String[] args) {
        BasicConfigurator.configure();
        try (DatagramSocket socket = new DatagramSocket(PORT);) {
            @SuppressWarnings("unchecked")
            MiddlewareDataStream<SensorData> stream = MiddlewareDataStreamFactory.getStream(getDataStreamClassName(),
                    getTableName());
            byte[] buffer = new byte[MAX_SIZE];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String jsonStr = new String(packet.getData());
                logPulseValue(jsonStr);

                socket.send(packet);
                stream.publish(SensorData.of(jsonStr));

            }
        } catch (Exception e) {
            logger.log("severe", e.toString());
        }

    }

    private static String getTableName() {
        return env.getOrDefault("STREAM_NAME", DEFAULT_PULSE_VALUES_STREAM);
    }

    private static String getDataStreamClassName() {
        return env.getOrDefault("DATA_STREAM_CLASS_NAME", DEFAULT_STREAM_CLASS_NAME);
    }

    private static void logPulseValue(String jsonStr) {
        logger.log("finest", jsonStr);
        SensorData sensorData = SensorData.of(jsonStr);
        int value = sensorData.value();
        if (value >= WARNING_LOG_VALUE && value <= ERROR_LOG_VALUE) {
            logValue("warning", sensorData);
        } else if (value > ERROR_LOG_VALUE) {
            logValue("error", sensorData);
        }
    }

    private static void logValue(String level, SensorData sensorData) {
        logger.log(level, String.format("patient %d has pulse value greater than %d", sensorData.patientId(),
                level.equals("warning") ? WARNING_LOG_VALUE : ERROR_LOG_VALUE));
    }

}