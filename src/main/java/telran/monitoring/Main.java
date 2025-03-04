package telran.monitoring;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import telran.monitoring.api.SensorData;
import telran.monitoring.logging.Logger;
import telran.monitoring.logging.LoggerStandard;

public class Main {

    private static final int PORT = 5000;
    private static final int MAX_SIZE = 1500;
    private static final int WARNING_LOG_VALUE = 220;
    private static final int ERROR_LOG_VALUE = 230;
    static Logger logger = new LoggerStandard("receiver");
    private static final String TABLE_NAME = "pulse_values";

    public static void main(String[] args) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(PORT);
                DynamoDbClient client = DynamoDbClient.builder().build()) {
            byte[] buffer = new byte[MAX_SIZE];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String jsonStr = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                logger.log("finest", jsonStr);
                logPulseValue(jsonStr);

                SensorData sensorData = SensorData.of(jsonStr);
                HashMap<String, AttributeValue> item = getMap(sensorData);

                PutItemRequest request = PutItemRequest.builder()
                        .tableName(TABLE_NAME)
                        .item(item)
                        .build();

                client.putItem(request);

            }
        }
    }

    private static void logPulseValue(String jsonStr) {
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

    private static HashMap<String, AttributeValue> getMap(SensorData sensorData) {
        HashMap<String, AttributeValue> map = new HashMap<>();
        map.put("patientId", AttributeValue.builder().n(String.valueOf(sensorData.patientId())).build());
        map.put("value", AttributeValue.builder().n(String.valueOf(sensorData.value())).build());
        map.put("timestamp", AttributeValue.builder().n(String.valueOf(sensorData.timestamp())).build());
        return map;
    }
}
