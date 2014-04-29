package com.continuuity.internal.app.runtime.batch;

import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.DataSetFieldSetter;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Wraps user-defined implementation of {@link Reducer} class which allows perform extra configuration.
 */
public class ReducerWrapper extends Reducer {

  public static final String ATTR_REDUCER_CLASS = "c.reducer.class";

  private static final Logger LOG = LoggerFactory.getLogger(MapperWrapper.class);

  private File unpackedJarDir;

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);

    if (unpackedJarDir != null) {
      try {
        FileUtils.deleteDirectory(unpackedJarDir);
      } catch (IOException e) {
        // NO-OP
      }
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    unpackedJarDir = Files.createTempDir();
    MapReduceContextProvider mrContextProvider =
      new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Reducer);
    final BasicMapReduceContext basicMapReduceContext = mrContextProvider.get(unpackedJarDir);
    basicMapReduceContext.getMetricsCollectionService().startAndWait();

    try {
      String userReducer = context.getConfiguration().get(ATTR_REDUCER_CLASS);
      Reducer delegate = createReducerInstance(basicMapReduceContext.getProgram().getClassLoader(), userReducer);

      // injecting runtime components, like datasets, etc.
      try {
        Reflections.visit(delegate, TypeToken.of(delegate.getClass()),
                          new PropertyFieldSetter(basicMapReduceContext.getSpecification().getProperties()),
                          new MetricsFieldSetter(basicMapReduceContext.getMetrics()),
                          new DataSetFieldSetter(basicMapReduceContext));
      } catch (Throwable t) {
        LOG.error("Failed to inject fields to {}.", delegate.getClass(), t);
        throw Throwables.propagate(t);
      }

      LoggingContextAccessor.setLoggingContext(basicMapReduceContext.getLoggingContext());

      // this is a hook for periodic flushing of changes buffered by datasets (to avoid OOME)
      WrappedReducer.Context flushingContext = createAutoFlushingContext(context, basicMapReduceContext);

      delegate.run(flushingContext);
      // sleep to allow metrics to be written
      TimeUnit.SECONDS.sleep(2L);

      // transaction is not finished, but we want all operations to be dispatched (some could be buffered in
      // memory by tx agent
      try {
        basicMapReduceContext.flushOperations();
      } catch (Exception e) {
        LOG.error("Failed to flush operations at the end of reducer of " + basicMapReduceContext.toString());
        throw Throwables.propagate(e);
      }
    } finally {
      basicMapReduceContext.close(); // closes all datasets
      basicMapReduceContext.getMetricsCollectionService().stop();
    }
  }

  private WrappedReducer.Context createAutoFlushingContext(final Context context,
                                                          final BasicMapReduceContext basicMapReduceContext) {

    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    final int flushFreq = context.getConfiguration().getInt("c.reducer.flush.freq", 10000);

    @SuppressWarnings("unchecked")
    WrappedReducer.Context flushingContext = new WrappedReducer().new Context(context) {
      private int processedRecords = 0;

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = super.nextKey();
        if (++processedRecords > flushFreq) {
          try {
            LOG.info("Flushing dataset operations...");
            basicMapReduceContext.flushOperations();
          } catch (Exception e) {
            LOG.error("Failed to persist changes", e);
            throw Throwables.propagate(e);
          }
          processedRecords = 0;
        }
        return result;
      }
    };
    return flushingContext;
  }

  private Reducer createReducerInstance(ClassLoader classLoader, String userReducer) {
    try {
      return (Reducer) classLoader.loadClass(userReducer).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance of the user-defined Reducer class: " + userReducer);
      throw Throwables.propagate(e);
    }
  }
}
