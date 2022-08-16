package com.example.projectxxx.PowerBI;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Controller {
    public void sendRequest() throws UnsupportedEncodingException {
        HttpPost post = new HttpPost("https://api.powerbi.com/beta/06f1b89f-07e8-464f-b408-ec1b45703f31/datasets/db067cf1-375c-424a-b789-efa75e1aebc6/rows?key=%2ByRfXg6Ok0O14O0NRIeG2THjYvsZSLQ40yrw2L07M6u7bdNSF1J5gfpebDwRyoctsmAn3xdfPEvKA09V5pT4BA%3D%3D");

        StringBuilder json = new StringBuilder();
        json.append("[");
        json.append("{");
        json.append("\"date\" :\"2022-08-13T08:40:49.384Z\",");
        json.append("\"index\" :\"TCB\",");
        json.append("\"reference_price\" :98.6,");
        json.append("\"opening_price\" :98.6,");
        json.append("\"closing_price\" :98.6,");
        json.append("\"max_price\" :98.6,");
        json.append("\"min_price\" :98.6,");
        json.append("\"average_price\" :98.6,");
        json.append("\"+-_variation\" :98.6,");
        json.append("\"%_variation\" :98.6,");
        json.append("\"matching_volume\" :98.6,");
        json.append("\"matching_value\" :98.6,");
        json.append("\"agreement_volume\" :98.6,");
        json.append("\"agreement_value\" :98.6,");
        json.append("\"total_volume\" :98.6,");
        json.append("\"total_value\" :98.6,");
        json.append("\"capitalization\" :98.6");
        json.append("}");
        json.append("]");

        //send a json data
        post.setEntity(new StringEntity(json.toString()));
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {

            String result = EntityUtils.toString(response.getEntity());
            System.out.println(result);
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
