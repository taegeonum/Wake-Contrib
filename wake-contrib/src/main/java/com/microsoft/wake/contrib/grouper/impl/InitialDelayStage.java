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
package com.microsoft.wake.contrib.grouper.impl;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.AbstractEStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.exception.WakeRuntimeException;
import com.microsoft.wake.impl.StageManager;
import com.microsoft.wake.rx.Observer;


/**
 * Stage that executes the observer after initial delay. 
 *
 * @param <T> type
 */
public final class InitialDelayStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(ContinuousStage.class.getName());

  private final Observer<T> handler;
  private final int numThreads;
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  private final AtomicBoolean done = new AtomicBoolean(false);
  private ScheduledExecutorService scheduler;

  
  /**
   * Constructs a stage that executes an event after initial delay. 
   *
   * @param handler   the observer to execute
   * @param numThreads the number of threads
   * @throws InjectionException 
   */
  
  @Inject
  public InitialDelayStage(@Parameter(StageConfiguration.StageObserver.class) Observer<T> handler,
      @Parameter(StageConfiguration.NumberOfThreads.class) int numThreads, 
      @Parameter(StageConfiguration.StageName.class) String name) throws InjectionException {
    
    super(name);
    this.handler = handler;
    if (numThreads <= 0)
      throw new WakeRuntimeException("numThreads " + numThreads + " is less than or equal to 0");

    this.numThreads = numThreads;
    this.scheduler = Executors.newScheduledThreadPool(1);

    LOG.info("Initial Delay Stage");
    StageManager.instance().register(this);
  }
  

  class OutputTask implements Runnable {
    private T delay;
    public OutputTask(T delay){
      this.delay = delay;
    }

    @Override
    public void run() {
      InitialDelayStage.this.handler.onNext(delay);
    }
  }

  /**
   * Starts the stage with new continuous event
   *
   * @param value the new value
   */
  @Override
  public void onNext(final T delay_ms) {
    beforeOnNext();
    final Long delay = (Long)delay_ms;
    scheduler.schedule(new OutputTask(delay_ms), delay, TimeUnit.MILLISECONDS);
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
        if (!scheduler.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
          List<Runnable> droppedRunnables = scheduler.shutdownNow();
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
          scheduler.shutdown();
          handler.onCompleted();
        }
      }
    };
  }

}
