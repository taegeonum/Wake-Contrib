package com.microsoft.wake.contrib.grouper.impl;
/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.commons.lang.NotImplementedException;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.contrib.grouper.Grouper;
import com.microsoft.wake.contrib.grouper.Tuple;
import com.microsoft.wake.metrics.Meter;
import com.microsoft.wake.rx.AbstractRxStage;
import com.microsoft.wake.rx.Observer;

public class AdaptiveSnowshovelGrouper<InType, OutType, K, V> extends AbstractRxStage<InType> implements Grouper<InType> {

  @NamedParameter(doc = "The maximum flushing period. (ms)", short_name = "max_period", default_value = "2000")
  public static final class MaxPeriod implements Name<Long> {}

  @NamedParameter(doc = "The minimum flushing period. (ms)", short_name = "min_period", default_value = "150")
  public static final class MinPeriod implements Name<Long> {}
  
  @NamedParameter(doc = "The interval of changing flushing period. (ms)", short_name = "interval", default_value = "50")
  public static final class Interval implements Name<Long> {}
  
  @NamedParameter(doc = "The initial flushing period. (ms)", short_name = "initial_period", default_value = "150")
  public static final class InitialPeriod implements Name<Long> {}
  
  private Logger LOG = Logger.getLogger(CombiningSnowshovelGrouper.class.getName());
  
  private ConcurrentSkipListMap<K, V> register;
  private volatile boolean inputDone;
  private Combiner<OutType,K, V> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 

  private final EStage<Long> outputDriver;
  private final EventHandler<Integer> doneHandler;
  
  private final AtomicInteger sleeping;

  private final OutputImpl<Long> outputHandler;
  
  private long prevFlushingPeriod;
  private long currFlushingPeriod;
  private long flushingPeriodInterval; // ms
  private long prevAdjustedTime;
  private long prevElapsedTime;
  private long startTime;
  
  private double currCombiningRate;
  private double prevCombiningRate; 

  //private AtomicLong currAggregatedCount;
  //private AtomicLong prevAggregatedCount;
  private long aggCntSnapshot;
  private long prevAggregatedCount;
  
  private final long minPeriod;
  private final long maxPeriod;
    
  private final Meter combiningMeter;

  
  /* 
   * Adaptive Snowshovel grouper 
   * It adjusts snowshovel flushing period in regard to the combining rate.
   * 
   * The flushing period increases by @interval if ( 
   * prevCombiningRate < currCombiningRate && prevFlushingPeriod < currFlushingPeriod ||
   * prevCombiningRate > currCombiningRate && prevFlushingPeriod > currFlushingPeriod ) 
   * The flushing period decreases by @interval otherwise. 
   * 
   * @minPeriod <= flushing period <= @maxPeriod
   * 
   * @param c   combiner
   * @param p   partitioner
   * @param ext   extractor
   * @param o   output observer
   * @param stageName   stageName 
   * @param initialPeriod   initial flushing period
   * @param minPeriod   minimum flushing period
   * @param maxPeriod   maximum flushing period
   * @param interval    adjusting interval
   * 
   */
  
  @Inject
  public AdaptiveSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.StageName.class) String stageName,
      @Parameter(InitialPeriod.class) long initialPeriod,
      @Parameter(MaxPeriod.class) long maxPeriod,
      @Parameter(MinPeriod.class) long minPeriod,
      @Parameter(Interval.class) long interval
      ) throws InjectionException {
    super(stageName);
    
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    this.outputHandler = new OutputImpl<Long>();
    this.outputDriver = new InitialDelayStage<Long>(outputHandler, 1, stageName+"-output");
    this.doneHandler = ((InitialDelayStage<Long>)outputDriver).getDoneHandler();
    register = new ConcurrentSkipListMap<>();
    inputDone = false;
    this.inputObserver = this.new InputImpl();
    this.sleeping = new AtomicInteger();
    this.combiningMeter = new Meter(stageName);

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking

    outputDriver.onNext(new Long(initialPeriod));
    prevAggregatedCount = 0;
    prevCombiningRate = currCombiningRate = 0.0;
    prevFlushingPeriod = 0;
    currFlushingPeriod = initialPeriod;
    prevAdjustedTime = startTime = System.nanoTime();
    
    flushingPeriodInterval = interval;
    this.minPeriod = minPeriod;
    this.maxPeriod = maxPeriod;
    
    
  }

  private interface Input<T> extends Observer<T>{}
  private class InputImpl implements Input<InType> {
    @Override
    public void onCompleted() {
      synchronized (register) {
        inputDone = true;
        register.notifyAll(); 
      }
      outputHandler.onNext(1L);
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
            if (LOG.isLoggable(Level.FINER)) {
              LOG.finer("input key:"+key+" val:"+val+" (new)");
            }
            break;
          }
        } else {
          newVal = c.combine(key, oldVal, val);
          succ = register.replace(key, oldVal, newVal);
          if (!succ)
            oldVal = register.get(key);
          else {
            if (LOG.isLoggable(Level.FINER)) {
              LOG.finer("input key:"+key+" val:"+val+" -> newVal:"+newVal);
            }
            break;
          }
        }
      } while (true);
      
      combiningMeter.mark(1L);
      
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
      
      long currTime = System.nanoTime();
      long elapsed = (currTime - prevAdjustedTime);
      aggCntSnapshot = combiningMeter.getCount();
      currCombiningRate = ((aggCntSnapshot - prevAggregatedCount) * 1000000000.0 / elapsed);
      double deltaCombiningRate = currCombiningRate - prevCombiningRate;
      long deltaPeriod = (long) (elapsed - prevElapsedTime);

      // + or - 
      int direction = sign(deltaCombiningRate) / sign(deltaPeriod);

      // update the prev values 
      prevAdjustedTime = currTime;
      prevElapsedTime = elapsed;
      prevFlushingPeriod = currFlushingPeriod;
      prevCombiningRate = currCombiningRate;
      currFlushingPeriod = Math.min(maxPeriod, Math.max(minPeriod, currFlushingPeriod + direction * flushingPeriodInterval));
      prevAggregatedCount = aggCntSnapshot;
      outputDriver.onNext(currFlushingPeriod);

    }
    
  }
  
  private int sign(long num){
    num = num == 0 ? 1 : num;
    return (int) (num / Math.abs(num));
  }
  
  private int sign(double num){
    num = num == 0 ? 1 : num;
    return (int) (num / Math.abs(num));
  }
  
  @Override
  public String toString() {
    
    // time aggregatedCount currFlushingPeriod prevCombiningRate currCombiningRate prevElapstedTime currElapsedTime"
    long currTime = System.nanoTime();
    long elapsed = (currTime - prevAdjustedTime);
    double elapsedTime = (currTime - startTime) / 1000000.0;

    StringBuilder sb = new StringBuilder();
    sb.append(elapsedTime).append("\t")
    .append(aggCntSnapshot).append("\t")
    .append(currFlushingPeriod).append("\t")
    .append(prevCombiningRate).append("\t")
    .append(currCombiningRate).append("\t")
    .append(prevElapsedTime/1000000).append("\t")
    .append(elapsed/1000000).append("\t");

    return sb.toString();
  }

  @Override
  public void close() throws Exception {
    this.outputDriver.close();
  }

  @Override
  public void onCompleted() {
    inputObserver.onCompleted();
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
