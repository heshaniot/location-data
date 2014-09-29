package org.wso2.cep;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.SystemDefaultHttpClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class LocationDataProducer {

    public static void main(String[] args) {
        String url = args[0];
        HttpClient httpClient = new SystemDefaultHttpClient();

        try {
            HttpPost method = new HttpPost(url);

            BufferedReader br = new BufferedReader(new FileReader("resources/input.csv"));

            String line = "";
            List<GeoJsonPointFeature> locationList = new ArrayList<GeoJsonPointFeature>();
            while ((line = br.readLine()) != null) {
                String[] location = line.split(",");
                String deviceId = location[0];
                String timeStamp = location[1];
                double longitude = Double.parseDouble(location[2]);
                double latitude = Double.parseDouble(location[3]);

                GeoJsonPointFeature geoJsonPointFeature =
                        new GeoJsonPointFeature(deviceId,
                                timeStamp);
                GeoJsonPoint geoJsonPoint = new GeoJsonPoint(longitude, latitude);
                geoJsonPointFeature.setGeometry(geoJsonPoint);
                locationList.add(geoJsonPointFeature);
            }

            if (httpClient != null) {
                int i = 0;
                for (GeoJsonPointFeature geoJsonPointFeature : locationList) {
                    StringEntity gpsEntity = new StringEntity(geoJsonPointFeature.getAsGeoJson());
                    gpsEntity.setContentType("application/json");
                    method.setEntity(gpsEntity);

                    httpClient.execute(method).getEntity().getContent().close();
                    System.out.println("Sent event no :" + i++ + " " +
                            geoJsonPointFeature.getAsGeoJson());

                }
                Thread.sleep(500);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
