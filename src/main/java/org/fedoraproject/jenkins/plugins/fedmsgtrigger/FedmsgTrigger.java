/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Red Hat, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.fedoraproject.jenkins.plugins.fedmsgtrigger;

import hudson.Extension;
import hudson.model.BuildableItem;
import hudson.model.Item;
import hudson.triggers.Trigger;
import hudson.triggers.TriggerDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import net.sf.json.JSONObject;

import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.StaplerRequest;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FedmsgTrigger extends Trigger<BuildableItem> {

    private final List<MsgCheck> checks;
    private final String hubAddr;
    private final String topic;
    private transient BuildableItem project;

    @DataBoundConstructor
    public FedmsgTrigger(String hubAddr, String topic, List<MsgCheck> checks) {
        this.hubAddr = hubAddr;
        this.topic = topic;
        if (checks == null) {
            checks = new ArrayList<MsgCheck>();
        }
        this.checks = checks;
    }

    public String getHubAddr() {
        return hubAddr;
    }

    public String getTopic() {
        return topic;
    }

    public List<MsgCheck> getChecks() {
        return Collections.unmodifiableList(checks);
    }

    @Override
    public void start(BuildableItem project, boolean newInstance) {
        this.project = project;
        ((DescriptorImpl) getDescriptor()).addTrigger(this);
    }

    @Override
    public void stop() {
        ((DescriptorImpl) getDescriptor()).deleteTrigger(this);
    }

    public void run(FedmsgMessage msg) {
        project.scheduleBuild(new FedmsgTriggerCause(msg));
    }

    @Extension
    public static final class DescriptorImpl extends TriggerDescriptor {

        private static final Logger LOGGER = Logger.getLogger(DescriptorImpl.class.getName());

        private Map<String, FedmsgListener> listeners = new HashMap<String, FedmsgListener>();

        public DescriptorImpl() {
            load();
        }

        @Override
        public String getDisplayName() {
            return "Fedmsg Trigger";
        }

        @Override
        public boolean configure(StaplerRequest req, JSONObject formData) throws FormException {
            save();
            return super.configure(req, formData);
        }

        @Override
        public boolean isApplicable(Item item) {
            return true;
        }

        public void addTrigger(FedmsgTrigger trigger) {
            // TODO: error/failure handling! this will need to be rewritten...

            synchronized (listeners) {
                FedmsgListener listener = listeners.get(trigger.getHubAddr());
                if (listener == null) {
                    listener = new FedmsgTrigger.FedmsgListener(trigger.getHubAddr());
                    listeners.put(trigger.getHubAddr(), listener);
                    Thread thread = new Thread(listener, trigger.getHubAddr());
                    thread.start();
                }
                listener.addConsumer(trigger);
            }
        }

        public void deleteTrigger(FedmsgTrigger trigger) {
            synchronized (listeners) {
                FedmsgListener listener = listeners.get(trigger.getHubAddr());
                if (listener == null) {
                    LOGGER.warning("Trying to remove non-existent listener for " + trigger.getHubAddr());
                    return;
                }
                listener.deleteConsumer(trigger);

                if (!listener.hasConsumers()) {
                    listeners.remove(trigger.getHubAddr());
                    listener.stop();
                }
            }
        }
    }

    private static class FedmsgListener implements Runnable {

        private Context ctx;
        private Socket subscriber;

        private static final Logger LOGGER = Logger.getLogger(FedmsgListener.class.getName());

        private List<FedmsgTrigger> consumers = Collections.synchronizedList(new ArrayList<FedmsgTrigger>());

        public FedmsgListener(String hubAddr) {
            this.ctx = ZMQ.context(1);
            this.subscriber = ctx.socket(ZMQ.SUB);

            LOGGER.fine("Connecting to " + hubAddr);
            subscriber.connect(hubAddr);
        }

        public void addConsumer(FedmsgTrigger consumer) {
            synchronized (consumers) {
                consumers.add(consumer);
                subscriber.subscribe(consumer.getTopic().getBytes());
            }
        }

        public void deleteConsumer(FedmsgTrigger consumer) {
            synchronized (consumers) {
                boolean unsubscribe = true;
                for (FedmsgTrigger trigger : consumers) {
                    if (trigger != consumer && consumer.getTopic().equals(trigger.getTopic())) {
                        unsubscribe = false;
                        break;
                    }
                }
                if (unsubscribe) {
                    subscriber.unsubscribe(consumer.getTopic().getBytes());
                }
                consumers.remove(consumer);
            }
        }

        public boolean hasConsumers() {
            return !consumers.isEmpty();
        }

        public void stop() {
            subscriber.close();
            ctx.term();
        }

        @Override
        public void run() {
            ObjectMapper mapper = new ObjectMapper();

            while (!Thread.currentThread().isInterrupted()) {
                String json = null;

                try {
                    json = subscriber.recvStr();

                    if (json == null) {
                        continue;
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        break;
                    }
                }

                // TODO: move to another thread
                try {
                    FedmsgMessage data = mapper.readValue(json, FedmsgMessage.class);
                    LOGGER.finest("received: " + data.getMsg().toString());

                    synchronized (consumers) {
                        for (FedmsgTrigger trigger : consumers) {
                            if (!trigger.getTopic().equals(data.getTopic())) {
                                continue;
                            }

                            boolean allPassed = true;
                            for (MsgCheck check : trigger.getChecks()) {
                                if (!check.check(data)) {
                                    allPassed = false;
                                    break;
                                }
                            }

                            if (allPassed) {
                                trigger.run(data);
                            }
                        }
                    }
                } catch (JsonParseException e) {
                    // TODO: not all received messages are in JSON format (from
                    // some reason), investigate more...
                } catch (JsonMappingException e) {
                    LOGGER.severe(e.toString());
                } catch (IOException e) {
                    LOGGER.severe(e.toString());
                }
            }
        }
    }
}