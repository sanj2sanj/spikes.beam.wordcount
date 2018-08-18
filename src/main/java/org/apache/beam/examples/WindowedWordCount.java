/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 * <p>
 * <p>This class, {@link WindowedWordCount}, is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at {@link MinimalWordCount},
 * {@link WordCount}, and {@link DebuggingWordCount}.
 * <p>
 * <p>Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally
 * and using a selected runner; defining DoFns;
 * user-defined PTransforms; defining PipelineOptions.
 * <p>
 * <p>New Concepts:
 * <pre>
 *   1. Unbounded and bounded pipeline input modes
 *   2. Adding timestamps to data
 *   3. Windowing
 *   4. Re-using PTransforms over windowed PCollections
 *   5. Accessing the window of an element
 *   6. Writing data to per-window text files
 * </pre>
 * <p>
 * <p>By default, the examples will run with the {@code DirectRunner}.
 * To change the runner, specify:
 * <pre>{@code
 *   --runner=YOUR_SELECTED_RUNNER
 * }
 * </pre>
 * See examples/java/README.md for instructions about how to configure different runners.
 * <p>
 * <p>To execute this pipeline locally, specify a local output file (if using the
 * {@code DirectRunner}) or output prefix on a supported distributed file system.
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 * <p>
 * <p>The input file defaults to a public data set containing the text of of King Lear,
 * by William Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 * <p>
 * <p>By default, the pipeline will do fixed windowing, on 1-minute windows.  You can
 * change this interval by setting the {@code --windowSize} parameter, e.g. {@code --windowSize=10}
 * for 10-minute windows.
 * <p>
 * <p>The example will try to cancel the pipeline on the signal to terminate the process (CTRL-C).
 */
public class WindowedWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedWordCount.class);


    /**
     * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
     * this example, for the bounded data case.
     * <p>
     * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
     * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
     * 2-hour period.
     */
    static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            LOG.info("min " + minTimestamp + ",max " + maxTimestamp);
        }


        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant randomTimestamp =
                    new Instant(
                            ThreadLocalRandom.current()
                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));

            /**
             * Concept #2: Set the data element with that timestamp.
             */
            c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
        }
    }

    /**
     * A {@link DefaultValueFactory} that returns the current system time.
     */
    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    /**
     * A {@link DefaultValueFactory} that returns the minimum timestamp plus one hour.
     */
    public static class DefaultToMinTimestampPlusOneMinute implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(Options.class).getMinTimestampMillis()
                    + Duration.standardSeconds((60) - 1).getMillis();
        }
    }


    /**
     * Options for {@link WindowedWordCount}.
     * <p>
     * <p>Inherits standard example configuration options, which allow specification of the
     * runner, as well as the {@link WordCount.WordCountOptions} support for
     * specification of the input and output files.
     */
    public interface Options extends WordCount.WordCountOptions,
            ExampleOptions, ExampleBigQueryTableOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(60)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneMinute.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("Fixed number of shards to produce per window, or null for runner-chosen sharding")
        @Default.Integer(1)
        Integer getNumShards();

        void setNumShards(Integer numShards);
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        final String output = options.getOutput();
        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

        Pipeline pipeline = Pipeline.create(options);

        /**
         * Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
         * unbounded input source.
         */
        PCollection<String> input = pipeline
                /** Read from the GCS file. */
                .apply(TextIO.read().from(options.getInputFile()))
                // Concept #2: Add an element timestamp, using an artificial time just to show windowing.
                // See AddTimestampFn for more detail on this.
                .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

        /**
         * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 1
         * minute (you can change this with a command-line option). See the documentation for more
         * information on how fixed windows work, and for information on the other types of windowing
         * available (e.g., sliding windows).
         */

        //Align the window to start with the element with the youngest timestamp
        //final Duration offset = Duration.standardSeconds(0);
        final Duration offset = Duration.standardSeconds(minTimestamp.toDateTime().getSecondOfMinute());

        PCollection<String> windowedWords =
                input.apply(
                        Window.<String>into(
                                FixedWindows.of(Duration.standardSeconds(options.getWindowSize())).withOffset(offset)));

        /**
         * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
         * windows over a PCollection containing windowed values.
         */
        PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new WordCount.CountWords());

        windowedWords.apply("Debug timestamps", new LoggingTransform());
        /**
         * Concept #5: Format the results and write to a sharded file partitioned by window, using a
         * simple ParDo operation. Because there may be failures followed by retries, the
         * writes must be idempotent, but the details of writing to files is elided here.
         */
        wordCounts
                .apply(MapElements.via(new WordCount.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(output, options.getNumShards()));

        PipelineResult result = pipeline.run();
        MetricResults metrics = result.metrics();
        MetricQueryResults metricResults = metrics.queryMetrics(MetricsFilter.builder().build());

        Iterable<MetricResult<Long>> counters = metricResults.counters();
        for (MetricResult<Long> counter : counters) {
            LOG.info("{}",counter);
            if (counter.name().name().equalsIgnoreCase("numLines")){
                assert counter.committed() == 5525;
            }
        }
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    private static class LoggingTransform extends org.apache.beam.sdk.transforms.PTransform<PCollection<String>, PCollection<String>> {

        private final Counter numLines = Metrics.counter(LoggingTransform.class, "numLines");

        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            lines.apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c, BoundedWindow window) {
                    LOG.info(window + " " + c.element());
                    numLines.inc();
                }
            }));
            return lines;
        }
    }

}
