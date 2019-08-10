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
import org.apache.beam.sdk.transforms.windowing.*;
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

public class ExactDollarRides {
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

  private static class TransformRides extends DoFn<Double, PubsubMessage> {
    TransformRides() {}

    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow window) {
      Double dollars = c.element();
      TableRow r = new TableRow();
      r.set("dollar_turnover", dollars);
      // the timing can be:
      // EARLY: the dollar amount is not yet final
      // ON_TIME: dataflow thinks the dollar amount is final but late data are still possible
      // LATE: late data has arrived
      r.set("dollar_timing", c.pane().getTiming()); // EARLY, ON_TIME or LATE
      r.set("dollar_window", window.start().getMillis() / 1000.0 / 60.0); // timestamp in fractional minutes

      LOG.info("Outputting $ value {}} at {} with marker {} for window {}",
        dollars.toString(), Instant.now().toString(), c.pane().getTiming().toString(), window.hashCode());
      Map<String, String> attributes = Collections.<String, String>emptyMap();
      try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
        buffer.write(CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), r));
        c.output(new PubsubMessage(buffer.toByteArray(), attributes));
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  public static void main(String[] args) {
    CustomPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("read from PubSub",
        PubsubIO.readMessages()
        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .withTimestampAttribute("ts"))

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
     
     .apply("fixed window & triggering", 
       Window.<Double>into(FixedWindows.of(Duration.standardMinutes(1)))
        .triggering(
          AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
          .accumulatingFiredPanes()
          .withAllowedLateness(Duration.standardMinutes(5)))

     .apply("sum whole window", Sum.doublesGlobally().withoutDefaults())

     .apply("format rides", ParDo.of(new TransformRides()))

     .apply("WriteToPubsub",
        PubsubIO.writeMessages()
        .to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));

    p.run();
  }
}
