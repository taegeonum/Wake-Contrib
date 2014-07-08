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
package com.microsoft.wake.contrib.synchronization;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.wake.contrib.synchronization.Phaser.Master;
import com.microsoft.wake.contrib.synchronization.Phaser.NumParticipants;
import com.microsoft.wake.contrib.synchronization.Phaser.ParticipantBuilder;
import com.microsoft.wake.contrib.synchronization.Phaser.Participants;
import com.microsoft.wake.contrib.synchronization.Phaser.PhaserCodec;
import com.microsoft.wake.remote.RemoteConfiguration;
import com.microsoft.wake.remote.RemoteConfiguration.ManagerName;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteIdentifierFactory;
import com.microsoft.wake.remote.RemoteManager;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;


public class PhaserTest {
  private static final Logger LOG = Logger.getLogger(PhaserTest.class.getName());

  @Rule
  public TestName name = new TestName();

  @Test
  public void testPhaser() throws Exception {
    System.out.println(name.getMethodName());



    Injector inj1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "44231");
      // TODO: remove 
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      inj1 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm1 = inj1.getInstance(RemoteManager.class);
    RemoteIdentifier id1 = rm1.getMyIdentifier();

    Injector inj2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 2");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "45321");
      // TODO: remove
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      inj2 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm2 = inj2.getInstance(RemoteManager.class);
    RemoteIdentifier id2 = rm2.getMyIdentifier();
    RemoteIdentifierFactory fac2 = inj2.getInstance(RemoteIdentifierFactory.class);

    Injector pi1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Participants.class, ParticipantBuilder.newBuilder()
          .add(id1.toString())
          .add(id2.toString())
          .build());
      cb.bindNamedParameter(Master.class, id1.toString());
      cb.bindNamedParameter(NumParticipants.class, "2");
      pi1 = inj1.forkInjector(cb.build());
      //pi1 = inj1.createChildInjector(cb.build());
    }

    Injector pi2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, id1.toString());
      pi2 = inj2.forkInjector(cb.build());
      // pi2.bindVolatileInstance(RemoteManager.class, rm2);
    }

    final Phaser dut2 = pi2.getInstance(Phaser.class);
    final Phaser dut1 = pi1.getInstance(Phaser.class);

    ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut1.signal();
          System.out.println("1 signaled");
          dut1.waitAll();
          System.out.println("1 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("1 exiting");
        }
      }
    });

    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut2.signal();
          System.out.println("2 signaled");

          dut2.waitAll();
          System.out.println("2 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("2 exiting");
        }
      }
    });

    e.shutdown();
    Assert.assertTrue(e.awaitTermination(10, TimeUnit.SECONDS));

    rm1.close();
    rm2.close();
  }

  @Test
  public void testDelayedRegistration() throws Exception {
    System.out.println(name.getMethodName());


    Injector inj1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1111");
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      inj1 = Tang.Factory.getTang().newInjector(cb.build());
    }
    RemoteManager rm1 = inj1.getInstance(RemoteManager.class);
    RemoteIdentifier id1 = rm1.getMyIdentifier();
    
    Injector inj2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 2");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, "1112");
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      inj2 = Tang.Factory.getTang().newInjector(cb.build());
    }
    
    RemoteManager rm2 = inj2.getInstance(RemoteManager.class);

    Injector pi1;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      /* 
       * Omitting the Participants field to allow delayed registration to trigger 
       */
      cb.bindNamedParameter(Master.class, id1.toString());
      cb.bindNamedParameter(NumParticipants.class, "2");
      pi1 = inj1.forkInjector(cb.build());
    }

    Injector pi2;
    {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, id1.toString());
      pi2 = inj2.forkInjector(cb.build());
    }

    final Phaser dut2 = pi2.getInstance(Phaser.class);
    final Phaser dut1 = pi1.getInstance(Phaser.class);

    ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut1.signal();
          System.out.println("1 signaled");
          dut1.waitAll();
          System.out.println("1 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("1 exiting");
        }
      }
    });

    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          dut2.signal();
          System.out.println("2 signaled");

          dut2.waitAll();
          System.out.println("2 finished waiting");
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail();
        } finally {
          System.out.println("2 exiting");
        }
      }
    });

    e.shutdown();
    Assert.assertTrue(e.awaitTermination(3, TimeUnit.SECONDS));

    rm1.close();
    rm2.close();
  }

  @Test
  public void testParentToMany() throws Exception {
    LOG.log(Level.FINE, "Starting: " + name.getMethodName());

    final int numChildren = 13;
    final int basePort = 1111;

    final Injector masterInjector;
    {
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest 1");
      cb.bindNamedParameter(RemoteConfiguration.Port.class, Integer.toString(basePort));
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      masterInjector = Tang.Factory.getTang().newInjector(cb.build());
    }
    final RemoteManager masterRemoteManager = masterInjector.getInstance(RemoteManager.class);
    final RemoteIdentifier masterIdentifier = masterRemoteManager.getMyIdentifier();

    final Injector[] childInjectors = new Injector[numChildren];
    final RemoteManager[] childRMs = new RemoteManager[numChildren];
    for (int t = 0; t < numChildren; t++) {
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(ManagerName.class, "PhaserTest " + (t + 2));
      cb.bindNamedParameter(RemoteConfiguration.Port.class, Integer.toString(basePort + 1 + t));
      cb.bindNamedParameter(RemoteConfiguration.MessageCodec.class, PhaserCodec.class);
      childInjectors[t] = Tang.Factory.getTang().newInjector(cb.build());
      childRMs[t] = childInjectors[t].getInstance(RemoteManager.class);
    }

    final Injector masterPhaserInjector;
    {
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      /* 
       * Omitting the Participants field to allow delayed registration to trigger 
       */
      cb.bindNamedParameter(Master.class, masterIdentifier.toString());
      cb.bindNamedParameter(NumParticipants.class, Integer.toString(numChildren));
      masterPhaserInjector = masterInjector.forkInjector(cb.build());
    }

    final Injector[] phaserInjectors = new Injector[numChildren];
    for (int t = 0; t < numChildren; t++) {
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Master.class, masterIdentifier.toString());
      phaserInjectors[t] = childInjectors[t].forkInjector(cb.build());
    }

    final Phaser dut1 = masterPhaserInjector.getInstance(Phaser.class);
    final Phaser[] childDuts = new Phaser[numChildren];
    for (int t = 0; t < numChildren; t++) {
      childDuts[t] = phaserInjectors[t].getInstance(Phaser.class);
    }

    final ExecutorService e = Executors.newCachedThreadPool();
    e.submit(new Runnable() {

      @Override
      public void run() {
        try {
          LOG.log(Level.FINE, "Master waiting.");
          dut1.waitAll();
          LOG.log(Level.FINE, "Master finished waiting.");
        } catch (final Exception e) {
          LOG.log(Level.SEVERE, "Exception while waiting on the master.", e);
          Assert.fail();
        } finally {
          LOG.log(Level.FINE, "Master exiting.");
        }
      }
    });

    for (int t = 0; t < numChildren; t++) {
      final int tt = t;
      e.submit(new Runnable() {

        @Override
        public void run() {
          try {
            childDuts[tt].signal();
            LOG.log(Level.FINE, "Signaled " + tt);

            childDuts[tt].waitAll();
            LOG.log(Level.FINE, "Finished waiting for " + tt);

          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Exception while signaling or waiting.", e);
            Assert.fail();
          } finally {
            LOG.log(Level.FINE, "Exiting.");
          }
        }
      });
    }

    e.shutdown();
    Assert.assertTrue(e.awaitTermination(3, TimeUnit.SECONDS));

    masterRemoteManager.close();
    for (final RemoteManager childRemoteManager : childRMs) {
      childRemoteManager.close();
    }
  }
  
  
}

