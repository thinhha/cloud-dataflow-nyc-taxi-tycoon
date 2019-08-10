/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.codelabs.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.coders.CoderException;
import com.google.codelabs.dataflow.utils.CustomPipelineOptions;
import java.time.Instant;
import java.util.Map;
import java.util.Collections;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DollarRides {
  private static final Logger LOG = LoggerFactory.getLogger(DollarRides.class);

  // ride format from PubSub
  // {
  //  "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  //  "latitude":40.66684000000033,
  //  "longitude":-73.83933000000202,
  //  "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  //  "meter_reading":14.270274,
  //  "meter_increment":0.019336415,
  //  "ride_status":"enroute",
  //  "passenger_count":2
  // }

  public static void main(String[] args) {
    CustomPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("read from PubSub",
        PubsubIO.readMessages()
        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .withTimestampAttribute("ts"))
     

    // Let us compute the per minute revenue that this taxi business generates. 
    // We will use a sliding window for this, repeated every three seconds so that 
    // the "per minute" revenue is constantly updated. Our data contains, for each 
    // taxi location, the taxi meter increment (field: meter_increment) since the 
    // last location. Summing these increments will give us a $ turnover for the 
    // entire business.
     .apply("sliding window",
        Window.into(
          SlidingWindows.of(Duration.standardSeconds(60)).every(Duration.standardSeconds(3))))

    // The transform that extracts the meter increment 
     .apply("extract meter increment",
        ParDo.of(new DoFn<PubsubMessage, Double>() {
            @ProcessElement
            public void processElement(@Element PubsubMessage input, OutputReceiver<Double> out) {
                TableRow tableRow = null;
                try {
                    tableRow =  CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), input.getPayload());
                } catch (Exception ex){
                    ex.printStackTrace();
                }
                out.output(Double.parseDouble(tableRow.get("meter_increment").toString()));
            }
        }))

    // "doubles" means that we are adding data of type double, i.e. the type of our meter increments.
    // "globally" means that we are summing over the entire window and not per key this time.
    // "withoutDefaults" controls whether an empty window returns zero or nothing but at present, 
    // "withoutDefaults" must be specified when summing globally over a window and the result returned 
    // for empty windows is nothing. The alternative is not yet implemented.
     .apply("sum whole window", Sum.doublesGlobally().withoutDefaults())

     .apply("format rides",
        ParDo.of(new DoFn<Double, PubsubMessage>() {
            @ProcessElement
            public void processElement(@Element Double x, OutputReceiver<PubsubMessage> out) {
                TableRow r = new TableRow();
                r.set("dollar_run_rate_per_minute", x);
                LOG.info("Outputting value ${} at {} ", x, Instant.now().toString());
                Map<String, String> attributes = Collections.<String, String>emptyMap();
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    buffer.write(CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), r));
                    out.output(new PubsubMessage(buffer.toByteArray(), attributes));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }))

     .apply("WriteToPubsub",
        PubsubIO.writeMessages()
        .to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));

    p.run();
  }
}
