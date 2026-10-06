package com.datapoc.iceberg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.TimerResult;

/**
 * Appends one JSON object per Iceberg {@link CommitReport} to a local file.
 *
 * <p>Register it on the Flink catalog with {@code metrics-reporter-impl}. The path comes from the
 * catalog property {@code commit-metrics.path}, then the {@code ICEBERG_COMMIT_METRICS_PATH}
 * environment variable, then {@code /var/iceberg-metrics/commits.jsonl}.
 *
 * <p>Failures are logged and swallowed so a metrics problem cannot fail an Iceberg commit.
 * Scan reports are ignored.
 */
public class JsonFileMetricsReporter implements MetricsReporter, Serializable {

  private static final long serialVersionUID = 1L;

  private String outputPath;
  private transient Object writeLock = new Object();

  private Object writeLock() {
    if (writeLock == null) {
      writeLock = new Object();
    }
    return writeLock;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    String path = properties == null ? null : properties.get("commit-metrics.path");
    if (path == null || path.isBlank()) {
      path = System.getenv("ICEBERG_COMMIT_METRICS_PATH");
    }
    if (path == null || path.isBlank()) {
      path = "/var/iceberg-metrics/commits.jsonl";
    }
    this.outputPath = path;
    try {
      Path file = Path.of(path);
      if (file.getParent() != null) {
        Files.createDirectories(file.getParent());
      }
      System.err.println("JsonFileMetricsReporter writing CommitReports to " + path);
    } catch (Exception e) {
      System.err.println("JsonFileMetricsReporter disabled: " + e.getMessage());
      this.outputPath = null;
    }
  }

  @Override
  public void report(MetricsReport report) {
    if (!(report instanceof CommitReport) || outputPath == null) {
      return;
    }
    try {
      String line = toJson((CommitReport) report);
      synchronized (writeLock()) {
        appendLine(line);
      }
    } catch (Exception e) {
      System.err.println("JsonFileMetricsReporter failed to record commit: " + e.getMessage());
    }
  }

  private void appendLine(String line) throws IOException {
    Path file = Path.of(outputPath);
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }
    try (FileChannel channel =
        FileChannel.open(
            file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND)) {
      try {
        channel.lock();
      } catch (IOException ignored) {
        // Bind mounts on Docker Desktop sometimes reject fcntl locks.
      }
      byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
      channel.write(ByteBuffer.wrap(bytes));
    }
  }

  static String toJson(CommitReport report) {
    CommitMetricsResult metrics = report.commitMetrics();
    StringBuilder json = new StringBuilder(512);
    json.append('{');
    field(json, "reported_at", Instant.now().toString());
    field(json, "table_name", report.tableName());
    field(json, "snapshot_id", report.snapshotId());
    field(json, "sequence_number", report.sequenceNumber());
    field(json, "operation", report.operation());
    field(json, "duration_ms", durationMs(metrics));
    field(json, "attempts", counter(metrics == null ? null : metrics.attempts()));
    field(json, "added_data_files", counter(metrics == null ? null : metrics.addedDataFiles()));
    field(json, "removed_data_files", counter(metrics == null ? null : metrics.removedDataFiles()));
    field(json, "total_data_files", counter(metrics == null ? null : metrics.totalDataFiles()));
    field(json, "added_delete_files", counter(metrics == null ? null : metrics.addedDeleteFiles()));
    field(json, "total_delete_files", counter(metrics == null ? null : metrics.totalDeleteFiles()));
    field(json, "added_records", counter(metrics == null ? null : metrics.addedRecords()));
    field(json, "removed_records", counter(metrics == null ? null : metrics.removedRecords()));
    field(json, "total_records", counter(metrics == null ? null : metrics.totalRecords()));
    field(
        json,
        "added_files_size_bytes",
        counter(metrics == null ? null : metrics.addedFilesSizeInBytes()));
    field(
        json,
        "removed_files_size_bytes",
        counter(metrics == null ? null : metrics.removedFilesSizeInBytes()));
    field(
        json,
        "total_files_size_bytes",
        counter(metrics == null ? null : metrics.totalFilesSizeInBytes()));
    json.append(",\"metadata\":");
    json.append(metadataJson(report.metadata()));
    json.append('}');
    return json.toString();
  }

  private static Double durationMs(CommitMetricsResult metrics) {
    if (metrics == null) {
      return null;
    }
    TimerResult timer = metrics.totalDuration();
    if (timer == null || timer.totalDuration() == null) {
      return null;
    }
    return timer.totalDuration().toNanos() / 1_000_000.0;
  }

  private static Long counter(CounterResult result) {
    return result == null ? null : result.value();
  }

  private static void field(StringBuilder json, String name, Object value) {
    if (json.length() > 1) {
      json.append(',');
    }
    json.append('"').append(name).append("\":");
    if (value == null) {
      json.append("null");
    } else if (value instanceof Number) {
      if (value instanceof Double || value instanceof Float) {
        json.append(String.format(java.util.Locale.ROOT, "%.3f", ((Number) value).doubleValue()));
      } else {
        json.append(value);
      }
    } else {
      json.append(quote(String.valueOf(value)));
    }
  }

  private static String metadataJson(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return "{}";
    }
    StringBuilder json = new StringBuilder();
    json.append('{');
    boolean first = true;
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (!first) {
        json.append(',');
      }
      first = false;
      json.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
    }
    json.append('}');
    return json.toString();
  }

  private static String quote(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder json = new StringBuilder(value.length() + 2);
    json.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          json.append("\\\"");
          break;
        case '\\':
          json.append("\\\\");
          break;
        case '\n':
          json.append("\\n");
          break;
        case '\r':
          json.append("\\r");
          break;
        case '\t':
          json.append("\\t");
          break;
        default:
          if (c < 0x20) {
            json.append(String.format("\\u%04x", (int) c));
          } else {
            json.append(c);
          }
      }
    }
    json.append('"');
    return json.toString();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    writeLock = new Object();
  }
}
