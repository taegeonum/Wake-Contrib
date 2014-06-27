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
package com.microsoft.wake.contrib.grouper.impl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.AbstractEStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.exception.WakeRuntimeException;
import com.microsoft.wake.impl.DefaultThreadFactory;
import com.microsoft.wake.impl.StageManager;
import com.microsoft.wake.rx.Observer;


/**
 * Stage that executes the observer with a thread pool
 *
 * @param <T> type
 */
public final class InitialDelayStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(ContinuousStage.class.getName());

  private final Observer<T> handler;
  private final int numThreads;
  private final DefaultThreadFactory tf;
  private final ExecutorService executor;
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  private final AtomicBoolean done = new AtomicBoolean(false);

  @NamedParameter(default_value="1000")
  public final static class DelayMS implements Name<Long>{}
  
  /**
   * Constructs a stage that continuously executes an event with specified number of threads
   *
   * @param handler   the observer to execute
   * @param numThreads the number of threads
   */
  
  @Inject
  public InitialDelayStage(@Parameter(StageConfiguration.StageObserver.class) Observer<T> handler,
      @Parameter(StageConfiguration.NumberOfThreads.class) int numThreads, 
      @Parameter(StageConfiguration.StageName.class) String name) {
    
    super(name);
    this.handler = handler;
    if (numThreads <= 0)
      throw new WakeRuntimeException("numThreads " + numThreads + " is less than or equal to 0");

    this.tf = new DefaultThreadFactory(name);
    this.executor = Executors.newFixedThreadPool(1);
    this.numThreads = numThreads;

    System.out.println("Initial Delay Stage");
    StageManager.instance().register(this);
  }


  /**
   * Starts the stage with new continuous event
   *
   * @param value the new value
   */
  @Override
  public void onNext(final T delay_ms) {
    beforeOnNext();
    final Integer delay = (Integer)delay_ms;
    //System.out.println("InitialDelayStage onNext, period: " + delay);
    
    executor.submit(new Runnable(){
      @Override
      public void run() {
        // initial delay
        try {
          Thread.sleep(delay.longValue());
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        InitialDelayStage.this.handler.onNext(delay_ms);
      }

    });
    afterOnNext();
  }


  /**
   * Closes the stage
   *
   * @return Exception
   */
  @Override
  public void close() throws Exception {  
    if (closed.compareAndSet(false, true)) {
      if (numThreads > 0) {
        executor.shutdown();
        if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
          List<Runnable> droppedRunnables = executor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
        }
      }
    }
  }

  public EventHandler<Integer> getDoneHandler() {
    return new EventHandler<Integer>() {
      @Override
      public void onNext(Integer id) {
        if(!done.getAndSet(true)){
          handler.onCompleted();
        }
      }
    };
  }

}
