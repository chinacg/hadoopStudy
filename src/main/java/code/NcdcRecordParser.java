package code;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private int year;
    private int airTemperature;
    private String quality;
    private String stationId;

    public void parse(String record) {
        year = Integer.parseInt(record.substring(15, 19));
        String airTemperatureString;
        // Remove leading plus sign as parseInt doesn't like them
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
        stationId = record.substring(0,22);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMa1formedTemperature() {
        return !quality.matches("[01459]");
    }

    public boolean IsMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public int getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getStationId(){return this.stationId; }
}
