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

import java.util.Date;

import hudson.model.Cause;

public class FedmsgTriggerCause extends Cause {

    private FedmsgMessage msg;

    public FedmsgTriggerCause(FedmsgMessage msg) {
        super();
        this.msg = msg;
    }

    @Override
    public String getShortDescription() {
        return "Build triggered by Fedmsg message: ("+ new Date((long) msg.getTimestamp() * 1000) + ") " + msg.getTopic();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FedmsgTriggerCause;
    }

    @Override
    public int hashCode() {
        return 5;
    }
}
