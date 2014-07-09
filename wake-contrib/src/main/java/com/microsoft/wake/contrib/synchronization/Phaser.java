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
package com.microsoft.wake.contrib.synchronization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteIdentifierFactory;
import com.microsoft.wake.remote.RemoteManager;
import com.microsoft.wake.remote.RemoteMessage;
import com.microsoft.wake.remote.impl.MultiCodec;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * One time use. After waitAll() returns, all calls to waitAll() will
 * eventually return
 * 
 * Should be replaced with groupcomm.AllReduce once
 * that library is ready.
 */
public class Phaser implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Phaser.class.getName());
  
  /**
   * remote events
   */
  private final RemoteManager remoteManager;
  
  /**
   * Identifier for the endpoint that is coordinating
   */
  private final RemoteIdentifier masterRID;
  
  /**
   * For sending signals to the master
   */
  private final EventHandler<SignalMessage> masterHandler;
  
  /**
   * Monitor for waiting
   */
  private final Signal signal;
  
  /**
   * number of participants that must signal() before waitForAll() returns
   */
  private final int num;
  
  /**
   * For sending wakeups to each of the participants
   */
  private final List<EventHandler<SignalDoneMessage>> participantsHandlers;
  
  /**
   * Intermediate stage between receiving a signal and sending a response.
   * Message handlers are not allowed to block.
   */
  private final EStage<Integer> sendingStage;
  
  /**
   * Identifier of this node
   */
  private final RemoteIdentifier selfRID;
  
  /**
   * number of signals received
   */
  private final AtomicInteger checkedIn;
  
  /**
   * Whether the participants will provide their own address
   */
  private final boolean delayedRegistration;

  /**
   * Factory for creating remote identifiers
   */
  private final RemoteIdentifierFactory idfac;
  
  // non masters don't care about participants
  @NamedParameter
  public static class Participants implements Name<String>{}
  
  @NamedParameter
  public static class Master implements Name<String>{}
  
  // non masters don't care about number of participants
  @NamedParameter(default_value="-1")
  public static class NumParticipants implements Name<Integer>{}
  
  private static class SignalMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String remoteId;
    public SignalMessage(RemoteIdentifier remoteId) {
      this.remoteId = remoteId.toString();
    }
  }
  private  static class SignalDoneMessage implements Serializable {
    private static final long serialVersionUID = 1L;
  }
  
  @Inject
  public Phaser(RemoteManager remoteManager,
                RemoteIdentifierFactory idfac,
      @Parameter(Master.class) String masterRID,
      @Parameter(NumParticipants.class) int numParticipants) {
    this(remoteManager, idfac, "", masterRID, numParticipants, true);
  }
  
  @Inject
  private Phaser(RemoteManager remoteManager,
                 RemoteIdentifierFactory idfac,
      @Parameter(Participants.class) String participants,
      @Parameter(Master.class) String masterRID, 
      @Parameter(NumParticipants.class) int numParticipants) {
    this(remoteManager, idfac, participants, masterRID, numParticipants, false);
  }
  
  private Phaser(RemoteManager remoteManager,
                 RemoteIdentifierFactory idfac,
                String participants,
                String masterRIDstr,
                int numParticipants,
                boolean delayedRegistration) {
    this.remoteManager = remoteManager;
    this.idfac = idfac;
    this.masterRID = idfac.getNewInstance(masterRIDstr);
    this.selfRID = remoteManager.getMyIdentifier();
    this.signal = new Signal();
    this.num = numParticipants;
    this.delayedRegistration = delayedRegistration;
   
    this.checkedIn = new AtomicInteger(0);
   
    if (this.masterRID.equals(selfRID)) {
      this.participantsHandlers = new ArrayList<>();
      if (!delayedRegistration) {
        for (String p : ParticipantBuilder.parse(participants)) {
          RemoteIdentifier pident = idfac.getNewInstance(p);
          this.participantsHandlers.add(remoteManager.getHandler(pident, SignalDoneMessage.class));
        }
      }
    } else {
      this.participantsHandlers = null;
    }
    
    this.sendingStage = new ThreadPoolStage<>(new SendHandler(), 1);
    this.masterHandler = remoteManager.getHandler(masterRID, SignalMessage.class);
    
    LOG.info("participant " + selfRID + " registering handler (master="+masterRID+")");
    remoteManager.registerHandler(SignalDoneMessage.class, new SignalDoneMessageHandler());
    if (masterRID.equals(selfRID)) {
      LOG.info("Master " + selfRID + " registering handler");
      remoteManager.registerHandler(SignalMessage.class, new SignalMessageHandler());
    }
    
    remoteManager.registerErrorHandler(new EventHandler<Exception>() {
      @Override
      public void onNext(Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }
  
  private class SendHandler implements EventHandler<Integer> {
    @Override
    public void onNext(Integer ignore) {
      LOG.info("sending wake message for all participants");
      synchronized (participantsHandlers) {
        for (EventHandler<SignalDoneMessage> pe : participantsHandlers) {
          pe.onNext(new SignalDoneMessage());
        }
      }
      
      // master also signals self (for cases where it is not a participant but wants to wait)
      signal.signal();
    }
  }
  
  public void signal() {
    LOG.info(selfRID+ " sending signal to "+masterRID);
    masterHandler.onNext(new SignalMessage(selfRID));
  }
  
  public void waitAll() throws InterruptedException {
    signal.waitFor();
  }

  private class SignalMessageHandler implements EventHandler<RemoteMessage<SignalMessage>> {

    @Override
    public void onNext(RemoteMessage<SignalMessage> m) {
      assert masterRID.equals(selfRID);
      

      int current = checkedIn.incrementAndGet();
      //System.out.println("received from " + m.getIdentifier() +" phaser count now " + current +" / "+num);
      if (delayedRegistration) {
        synchronized (participantsHandlers) {
          participantsHandlers.add(remoteManager.getHandler(Phaser.this.idfac.getNewInstance(m.getMessage().remoteId), SignalDoneMessage.class));
        }
      }
      LOG.info("received from " + m.getIdentifier() +" phaser count now " + current +" / "+num);
      if (current == num) {
        sendingStage.onNext(1);
      }
    }
  }

  private class SignalDoneMessageHandler implements EventHandler<RemoteMessage<SignalDoneMessage>> {

    @Override
    public void onNext(RemoteMessage<SignalDoneMessage> m) {
      LOG.info("local phaser for " + selfRID+" received a wakeup");
      signal.signal();
    }
  }
  
  public static class ParticipantBuilder {
    private final StringBuilder sb;
    private static final String delim = " ";
    private ParticipantBuilder() {
      this.sb = new StringBuilder();
    }
    public ParticipantBuilder add(String participant) {
      sb.append(participant);
      sb.append(delim);
      return this;
    }
    public String build() {
      return sb.toString();
    }
    public static List<String> parse(String pList) {
      ArrayList<String> r = new ArrayList<>();
      Scanner sc = new Scanner(pList);
      sc.useDelimiter(delim);
      
      while (sc.hasNext()) {
        String g = sc.next();
        r.add(g);
      }
      sc.close();
      
      return r;
    }
    
    public static ParticipantBuilder newBuilder() {
      return new ParticipantBuilder();
    }
  }

  @Override
  public void close() throws Exception {
    sendingStage.close();
    
  }
  
  

  // TODO: remove
  public static class PhaserCodec implements Codec<Object> {
    
    private final MultiCodec<Object> mc;
    private Map<Class<? extends Object>, Codec<? extends Object>> clazzToCodecMap;
    
    @Inject
    public PhaserCodec(){
      this.clazzToCodecMap = new HashMap<Class<? extends Object>, Codec<? extends Object>>();
      clazzToCodecMap.put(SignalMessage.class, new SignalMessageCodec());
      clazzToCodecMap.put(SignalDoneMessage.class, new SignalDoneMessageCodec());
      this.mc = new MultiCodec<Object>(clazzToCodecMap);

    }
    
    public class SignalMessageCodec extends ObjectSerializableCodec<SignalMessage>{
    }
    
    public class SignalDoneMessageCodec extends ObjectSerializableCodec<SignalDoneMessage>{
    }
    
    @Override
    public byte[] encode(Object paramT) {
      return mc.encode(paramT);
    }

    @Override
    public Object decode(byte[] paramArrayOfByte) {
      return mc.decode(paramArrayOfByte);
    }
  }
}




