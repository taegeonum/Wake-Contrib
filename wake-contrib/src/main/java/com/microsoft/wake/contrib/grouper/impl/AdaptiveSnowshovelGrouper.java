package com.microsoft.wake.contrib.grouper.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.commons.lang.NotImplementedException;

import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.contrib.grouper.Grouper;
import com.microsoft.wake.contrib.grouper.GrouperEvent;
import com.microsoft.wake.contrib.grouper.Tuple;
import com.microsoft.wake.contrib.grouper.Grouper.Combiner;
import com.microsoft.wake.contrib.grouper.Grouper.Extractor;
import com.microsoft.wake.contrib.grouper.Grouper.Partitioner;
import com.microsoft.wake.rx.AbstractRxStage;
import com.microsoft.wake.rx.Observer;

public class AdaptiveSnowshovelGrouper<InType, OutType, K, V> extends AbstractRxStage<InType> implements Grouper<InType> {

  @NamedParameter(doc = "The maximum flushing period. (ms)", short_name = "max_period", default_value = "300")
  public static final class MaxPeriod implements Name<Integer> {}

  @NamedParameter(doc = "The minimum flushing period. (ms)", short_name = "min_period", default_value = "0")
  public static final class MinPeriod implements Name<Integer> {}
  
  @NamedParameter(doc = "The maximum aggregated count of inputs per sec. (inputs/sec)", short_name = "max_aggregated_count", default_value = "500000")
  public static final class MaxAggregatedCount implements Name<Integer> {}
  
  
  @DefaultImplementation(LinearAdaptiveFunction.class)
  public interface AdaptiveFunction {
    
    public abstract int valueAt(int inputsPerSec);
  }
  
  public static class LinearAdaptiveFunction implements AdaptiveFunction {
    
    // y = ax + b
    private final float a;
    private final int b;
    
    @Inject
    public LinearAdaptiveFunction(@Parameter(MinPeriod.class) int minPeriod, 
        @Parameter(MaxPeriod.class) int maxPeriod,
        @Parameter(MaxAggregatedCount.class) int maxCombinedCount){
      
      this.b = minPeriod;
      this.a = (maxPeriod - minPeriod) / (float)(maxCombinedCount);
    }
    
    @Override
    public int valueAt(int inputsPerSec) {
      return (int)(a * inputsPerSec + b);
    }
  }
  
  public static class SqrtScaleAdaptiveFunction implements AdaptiveFunction {
    
    // y = ax + b
    private final float a;
    private final int b;
    
    @Inject
    public SqrtScaleAdaptiveFunction(@Parameter(MinPeriod.class) int minPeriod, 
        @Parameter(MaxPeriod.class) int maxPeriod,
        @Parameter(MaxAggregatedCount.class) int maxCombinedCount){
      
      this.b = minPeriod;
      this.a = (maxPeriod - minPeriod) / (float)(Math.sqrt(maxCombinedCount));
    }
    
    @Override
    public int valueAt(int inputsPerSec) {
      return (int)(a * Math.sqrt(inputsPerSec) + b);
    }
  }
  
  public static class ExponentialScaleAdaptiveFunction implements AdaptiveFunction {
    
    // y = ax + b
    private final float a;
    private final int b;
    
    @Inject
    public ExponentialScaleAdaptiveFunction(@Parameter(MinPeriod.class) int minPeriod, 
        @Parameter(MaxPeriod.class) int maxPeriod,
        @Parameter(MaxAggregatedCount.class) int maxCombinedCount){
      
      this.b = minPeriod;
      this.a = (maxPeriod - minPeriod) / (float)(Math.pow(maxCombinedCount, 2));
    }
    
    @Override
    public int valueAt(int inputsPerSec) {
      return (int)(a * Math.pow(inputsPerSec,2) + b);
    }
  }
  
  private Logger LOG = Logger.getLogger(CombiningSnowshovelGrouper.class.getName());
  
  private ConcurrentSkipListMap<K, V> register;
  private volatile boolean inputDone;
  private Combiner<OutType,K, V> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 

  private final EStage<Integer> outputDriver;
  private final EventHandler<Integer> doneHandler;
  
  private final AtomicInteger sleeping;

  private final OutputImpl<Integer> outputHandler;

  private AdaptiveFunction func;
  

  private final int WINDOW_SIZE = 1000; // ms 
  
  
  private int prevCombiningRate;
  private int prevFlushingPeriod;
  
  private int currCombiningRate;
  private int currFlushingPeriod;
  
  private int flushingPeriodInterval; // ms
  private int adjustingInterval; // ms 
  private long prevAdjustedTime;
  
  
  private int currWindowSize; // ms
  private int prevWindowSize; // ms
  
  private AtomicInteger currAggregatedCount;
  private int prevAggregatedCount;
  private final Boolean lock = new Boolean(false);
  
  /* 
   * Adaptive Snowshovel grouper 
   * It adjusts snowshovel flushing period in regard to the combined count.
   *
   * @param c   combiner
   * @param p   partitioner
   * @param ext   extractor
   * @param o   output observer
   * @param stageName   stageName 
   * @param minPeriod   minimum period
   * @param maxPeriod   maximum period
   * @param maxAggregatedCount    maximum aggregated count of inputs per sec
   * @param func        adaptive function 
   * 
   */
  
  @Inject
  public AdaptiveSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.StageName.class) String stageName,
      @Parameter(MaxPeriod.class) int maxPeriod,
      @Parameter(MinPeriod.class) int minPeriod,
      @Parameter(MaxAggregatedCount.class) int maxAggregatedCount,
      AdaptiveFunction func
      ) {
    super(stageName);
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    this.func = func;
    this.outputHandler = new OutputImpl<Integer>();
    // calling this.new on a @Unit's inner class without its own state is currently the same as Tang injecting it
    this.outputDriver = new InitialDelayStage<Integer>(outputHandler, 1, stageName+"-output");
    this.doneHandler = ((InitialDelayStage<Integer>)outputDriver).getDoneHandler();
    register = new ConcurrentSkipListMap<>();
    inputDone = false;
    this.inputObserver = this.new InputImpl();
    
    this.sleeping = new AtomicInteger();

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking
    
    System.out.println("<!--");
    System.out.println("Adaptive period");
    outputDriver.onNext(new Integer((maxPeriod + minPeriod) / 2));
    
    //currWindowSize = new AtomicInteger(maxPeriod + minPeriod / 2);
    currWindowSize = maxPeriod + minPeriod / 2;
    //prevWindowSize = new AtomicInteger(0);
    prevWindowSize = 0;
    //prevAggregatedCount = new AtomicInteger(0);
    currAggregatedCount = new AtomicInteger(0);
    
    prevCombiningRate = 0;
    prevFlushingPeriod = 0;
    currFlushingPeriod = (maxPeriod + minPeriod) / 2;
    flushingPeriodInterval = 25;
    adjustingInterval = 1000;
    prevAdjustedTime = System.nanoTime();
    
  }

  private interface Input<T> extends Observer<T>{}
  private class InputImpl implements Input<InType> {
    @Override
    public void onCompleted() {
      synchronized (register) {
        inputDone = true;
        register.notifyAll();
      }
      outputHandler.onNext(0);
    }

    @Override
    public void onError(Exception arg0) {
      // TODO
      throw new NotImplementedException(arg0);
    }

    @Override
    public void onNext(InType datum) {
      V oldVal;
      V newVal;

      final K key = ext.key(datum);
      final V val = ext.value(datum);

      // try combining atomically until succeed
      boolean succ = false;
      oldVal = register.get(key);
      do {
        if (oldVal == null) {
          succ = (null == (oldVal = register.putIfAbsent(key, val)));
          if (succ) {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val+" (new)");
            break;
          }
        } else {
          newVal = c.combine(key, oldVal, val);
          succ = register.replace(key, oldVal, newVal);
          if (!succ)
            oldVal = register.get(key);
          else {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val+" -> newVal:"+newVal);
            currAggregatedCount.incrementAndGet();
            break;
          }
        }
      } while (true);
      

      // TODO: make less conservative
      if (sleeping.get() > 0) {
        synchronized (register) {
          register.notify();
        }
      }
     
      
    }
  }

  
 
  private interface Output<T> extends Observer<T> {}
  private  class OutputImpl<T> implements Output<T> {
    @Override
    public void onCompleted() {
      if (!register.isEmpty() && inputDone) {
        throw new IllegalStateException("Output channel cannot complete before outputting is finished");
      }

      o.onCompleted();
    }

    @Override
    public void onError(Exception ex) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException(ex);
    }

    /**
     * Best effort flush of current storage to output. Blocks until it flushes
     * something or until eventually after {@code InObserver.onCompleted()}
     * has been called.
     */
    @Override
    public void onNext(T flushingPeriod) {
      boolean flushedSomething = false;

      do {
        // quick check for empty
        if (register.isEmpty()) {
          // if it may be empty now then wait until filled
          sleeping.incrementAndGet();
          synchronized (register) {
            // if observed empty and done then finished outputting

            while (register.isEmpty() && !inputDone) {
              try {
                //long tag = Thread.currentThread().getId();// System.nanoTime();
                //LOG.finer("output side waits "+tag);
                register.wait();
                //LOG.finer("output side wakes "+tag);
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            }
          }
          sleeping.decrementAndGet();
          if (inputDone) {
            doneHandler.onNext(0);
            return;
          }
        }
        
        Map.Entry<K, V> e_cursor = register.pollFirstEntry();
        Tuple<K, V> cursor = (e_cursor == null) ? null : new Tuple<>(e_cursor.getKey(), e_cursor.getValue());
        while (cursor != null) {
          if (cursor.getValue() != null) {
            afterOnNext();
            o.onNext(new Tuple<>(p.partition(cursor.getKey()), c.generate(cursor.getKey(), cursor.getValue())));
            flushedSomething = true;
          }

          K nextKey = register.higherKey(cursor.getKey());

          // remove may return null if another thread interleaved a removal
          cursor = (nextKey == null) ? null : new Tuple<>(nextKey, register.remove(nextKey));
        }
      } while (!flushedSomething);

      
      // Adjust period
      /*
      if( prevAdjustedTime + adjustingInterval < System.nanoTime()){
        int aggCntSnapshot = currAggregatedCount.get();
        int currCombiningRate = (int)(aggCntSnapshot / (float)adjustingInterval);
        int deltaCombiningRate = currCombiningRate - prevCombiningRate;
        int direction = (currFlushingPeriod - prevFlushingPeriod) / Math.abs(currFlushingPeriod - prevFlushingPeriod);

        System.out.println("prevCombiningRate: " + prevCombiningRate + ", currCombiningRate: " + currCombiningRate + ", period: " + currFlushingPeriod);


        // change the values 
        prevFlushingPeriod = currFlushingPeriod;
        prevCombiningRate = currCombiningRate;
        if( deltaCombiningRate < 0) {
          currFlushingPeriod += -direction * flushingPeriodInterval;
        }else{
          currFlushingPeriod += direction * flushingPeriodInterval;
        }

        currAggregatedCount.addAndGet(-aggCntSnapshot);
        prevAdjustedTime = System.nanoTime();
      }
      */

      // Adjust period
      int currAggSnapshot = currAggregatedCount.get();
      if(currWindowSize < WINDOW_SIZE){
        currCombiningRate = (currAggSnapshot * 1000) / (currWindowSize);

        prevAggregatedCount = currAggSnapshot;
        prevWindowSize = currWindowSize;
      }else{

        int slidingBottom = currWindowSize - WINDOW_SIZE;
        int minus = (int) (((float)slidingBottom / prevWindowSize) * prevAggregatedCount);

        currCombiningRate = (currAggSnapshot - minus) * 1000 / WINDOW_SIZE;
        prevAggregatedCount = currAggSnapshot - minus;
        currAggregatedCount.getAndAdd(-minus);
        prevWindowSize = (WINDOW_SIZE);
      }

      int deltaCombiningRate = currCombiningRate - prevCombiningRate;
      int direction = (currFlushingPeriod - prevFlushingPeriod) / Math.abs(currFlushingPeriod - prevFlushingPeriod);

      System.out.println("prevCombiningRate: " + prevCombiningRate + ", currCombiningRate: " + currCombiningRate + ", period: " + currFlushingPeriod);
      prevFlushingPeriod = currFlushingPeriod;
      prevCombiningRate = currCombiningRate;

      int changeMovement = (deltaCombiningRate / Math.abs(deltaCombiningRate));
      currFlushingPeriod = Math.max(5, currFlushingPeriod + changeMovement * direction * flushingPeriodInterval);
      currWindowSize = (Math.min(currWindowSize, WINDOW_SIZE) + currFlushingPeriod);

      outputDriver.onNext(currFlushingPeriod);
      
    }
  }
  
  @Override
  public String toString() {
    return "register: "+register;
  }

  @Override
  public void close() throws Exception {
    this.outputDriver.close();
  }

  @Override
  public void onCompleted() {
    inputObserver.onCompleted();
    System.out.println("--!>");
  }
  @Override
  public void onError(Exception arg0) {
    inputObserver.onCompleted();
  }
  @Override
  public void onNext(InType arg0) {
    beforeOnNext();
    inputObserver.onNext(arg0);
  }

}
