package com.microsoft.wake.contrib.grouper.impl;
/**
 * Copyright (C) 2013 Microsoft Corporation
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

import com.microsoft.wake.contrib.grouper.Tuple;
import com.microsoft.wake.contrib.grouper.Grouper;
import com.microsoft.wake.contrib.grouper.GrouperEvent;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.AbstractRxStage;
import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.runtime.RuntimeClock;

import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO: document Comparable requirement on K for the skip list implementation
public class CombiningSnowshovelGrouper<InType, OutType, K, V> extends AbstractRxStage<InType> implements Grouper<InType> {
  private Logger LOG = Logger.getLogger(CombiningSnowshovelGrouper.class.getName());
  
  private ConcurrentSkipListMap<K, V> register;
  private volatile boolean inputDone;
  private Combiner<OutType,K, V> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 

  private final EStage<Object> outputDriver;
  private final EventHandler<Integer> doneHandler;
  
  private final AtomicInteger sleeping;

  private final OutputImpl outputHandler;
  
  // TODO: remove
  private final AtomicInteger combinedCount = new AtomicInteger(0);
  private final AtomicInteger prevCombinedCount = new AtomicInteger(0);
  private  long startTime;
  private  long endTime;
  
  private long prevAdjustedTime;

  private RuntimeClock clock;

  private ThreadPoolStage<Alarm> stage;
  
  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName,
      @Parameter(ContinuousStage.PeriodNS.class) long outputPeriod_ns) {
    super(stageName);
    
    // event checker 
    try {
      clock = buildClock();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    new Thread(clock).start();
    final EventsCountChecker checker = new EventsCountChecker(clock, 500);
    stage = new ThreadPoolStage<>(checker, 1);
    stage.onNext(null);
    
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    this.outputHandler = new OutputImpl();
    // calling this.new on a @Unit's inner class without its own state is currently the same as Tang injecting it
    this.outputDriver = new ContinuousStage<Object>(outputHandler, outputThreads, stageName+"-output", outputPeriod_ns);
    this.doneHandler = ((ContinuousStage<Object>)outputDriver).getDoneHandler();
    register = new ConcurrentSkipListMap<>();
    inputDone = false;
    this.inputObserver = this.new InputImpl();
    
    this.sleeping = new AtomicInteger();

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking
    outputDriver.onNext(new GrouperEvent());
    // TODO: remove
    startTime = prevAdjustedTime = System.nanoTime();
    
    
    // TODO: remove
    System.out.println("<!--");
    System.out.println("snow-" + outputPeriod_ns + "_combiningRate");
    System.out.println("# time  aggregatedCount  elapsed_time currCombiningRate");
  }

  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName) {
    this(c, p, ext, o, outputThreads, stageName, 0);
  }

  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads) {
    this(c, p, ext, o, outputThreads, CombiningSnowshovelGrouper.class.getName()+"-Stage");
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
            break;
          }
        }
      } while (true);
      
      // TODO: remove
      combinedCount.incrementAndGet();

      // TODO: make less conservative
      if (sleeping.get() > 0) {
        synchronized (register) {
          register.notify();
        }
      }
      
      

     
      /*if (val instanceof Integer) {
        LOG.info("snow shovel size "+register.size());
      }*/

      // notify at least the first time that consuming is possible
      //outputDriver.onNext(new GrouperEvent());
      
    }   
  }

  
 
  private interface Output<T> extends Observer<T> {}
  private class OutputImpl implements Output<Integer> { //TODO: could change Integer to a StageContext type since no effect
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
    public void onNext(Integer threadId) {
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
            //TODO: remove
            endTime = System.nanoTime();
            
            System.out.println("# combiningRate: " + combinedCount.get() / ((endTime - startTime) / 1000000000.0) + ", combinedCount: " + combinedCount.get());
            System.out.println("-->");
            doneHandler.onNext(threadId);
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
      

      
      //System.out.println("acutual elapsed_time:" + (System.nanoTime() - prevAdjustedTime)/1000000.0);
    }
  }
  
  // TODO: remove
  private class EventsCountChecker implements EventHandler<Alarm> {

    private final RuntimeClock clock;
    private final int duration;

    public EventsCountChecker(RuntimeClock clock, Integer eventsCheckingDuration) {
      this.clock = clock;
      this.duration = eventsCheckingDuration;

    }

    @Override
    public void onNext(final Alarm value) {
      int cntSnapshot = combinedCount.get();
      long currTime = System.nanoTime();
      int elapsedTime = (int) ((currTime - startTime) / 1000000.0);
      int actualElapsedTime = (int) ((currTime - prevAdjustedTime) / 1000000.0);
      System.out.println(elapsedTime + "\t" + cntSnapshot + "\t" +  actualElapsedTime + "\t" + (cntSnapshot - prevCombinedCount.get()) * 1000.0 / actualElapsedTime );
      
      prevCombinedCount.set(cntSnapshot);
      prevAdjustedTime = currTime;
      clock.scheduleAlarm(duration, this);
    }
  }
  
  // TODO: remove
  private static RuntimeClock buildClock() throws Exception {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang()
        .newConfigurationBuilder();

    final Injector injector = Tang.Factory.getTang()
        .newInjector(builder.build());

    return injector.getInstance(RuntimeClock.class);
  }
  
  
  @Override
  public String toString() {
    return "register: "+register;
  }

  @Override
  public void close() throws Exception {
    this.outputDriver.close();
    stage.close();
    clock.close();
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
