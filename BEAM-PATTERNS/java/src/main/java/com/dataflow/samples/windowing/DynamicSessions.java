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
package org.apache.beam.sdk.transforms.windowing;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.Duration;
import org.json.JSONObject;

/**
 * A {@link WindowFn} that windows values into sessions separated by periods with no input for at
 * least the duration specified by {@link #getGapDuration()}.
 *
 * <p>For example, in order to window data into session with at least 10 minute gaps in between
 * them:
 *
 * <pre>{@code
 * PCollection<Integer> pc = ...;
 * PCollection<Integer> windowed_pc = pc.apply(
 *   Window.<Integer>into(DynamicSessions
 *     .withDefaultGapDuration(Duration.standardMinutes(10))
 *     .withGapAttribute("gap")));
 * }</pre>
 */
public class DynamicSessions extends WindowFn<Object, IntervalWindow> {
  /** Duration of the gaps between sessions. */
  private final Duration gapDuration;

    /** Pub/Sub attribute that modifies session gap. */
  private final String gapAttribute;

  /** Creates a {@code DynamicSessions} {@link WindowFn} with the specified gap duration. */
  public static DynamicSessions withDefaultGapDuration(Duration gapDuration) {
    return new DynamicSessions(gapDuration, "");
  }

  public DynamicSessions withGapAttribute(String gapAttribute) {
    return new DynamicSessions(gapDuration, gapAttribute);
  }

  /** Creates a {@code DynamicSessions} {@link WindowFn} with the specified gap duration. */
  private DynamicSessions(Duration gapDuration, String gapAttribute) {
    this.gapDuration = gapDuration;
    this.gapAttribute = gapAttribute;
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    // Assign each element into a window from its timestamp until gapDuration in the
    // future.  Overlapping windows (representing elements within gapDuration of
    // each other) will be merged.
    Duration dataDrivenGap;
    JSONObject message = new JSONObject(c.element().toString());

    try {
      dataDrivenGap = Duration.standardSeconds(Long.parseLong(message.getString(gapAttribute)));
    }
    catch(Exception e) {
      dataDrivenGap = gapDuration;
    }
    return Arrays.asList(new IntervalWindow(c.timestamp(), dataDrivenGap));
  }

  @Override
  public void mergeWindows(MergeContext c) throws Exception {
    MergeOverlappingIntervalWindows.mergeWindows(c);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return other instanceof DynamicSessions;
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "%s is only compatible with %s.",
              DynamicSessions.class.getSimpleName(), DynamicSessions.class.getSimpleName()));
    }
  }

  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    throw new UnsupportedOperationException("DynamicSessions is not allowed in side inputs");
  }

  public Duration getGapDuration() {
    return gapDuration;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("gapDuration", gapDuration).withLabel("Default Session Gap Duration"));
    builder.add(DisplayData.item("gapAttribute", gapAttribute).withLabel("Gap Attribute"));
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof DynamicSessions)) {
      return false;
    }
    DynamicSessions other = (DynamicSessions) object;
    return getGapDuration().equals(other.getGapDuration());
  }

  @Override
  public int hashCode() {
    return Objects.hash(gapDuration);
  }
}