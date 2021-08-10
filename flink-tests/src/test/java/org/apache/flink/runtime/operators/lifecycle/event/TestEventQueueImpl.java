/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle.event;

import org.apache.flink.util.function.RunnableWithException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

class TestEventQueueImpl implements TestEventQueue {
    private final List<TestEvent> events = new CopyOnWriteArrayList<>();
    private final List<TestEventListener> listeners = new CopyOnWriteArrayList<>();

    public void add(TestEvent e) {
        events.add(e);
        listeners.forEach(l -> l.onEvent(e));
    }

    @Override
    public void withHandler(TestEventHandler handler) throws Exception {
        BlockingQueue<TestEvent> queue = new LinkedBlockingQueue<>();
        withListener(
                queue::add,
                () -> {
                    TestEventHandler.TestEventNextAction next =
                            TestEventHandler.TestEventNextAction.CONTINUE;
                    while (next == TestEventHandler.TestEventNextAction.CONTINUE) {
                        next = handler.handle(queue.take());
                    }
                });
    }

    public void withListener(TestEventListener listener, RunnableWithException action)
            throws Exception {
        listener = addListener(listener);
        try {
            action.run();
        } finally {
            removeListener(listener);
        }
    }

    public TestEventListener addListener(TestEventListener listener) {
        listeners.add(listener);
        return listener;
    }

    public void removeListener(TestEventListener listener) {
        listeners.remove(listener);
    }

    @Override
    public List<TestEvent> getAll() {
        return Collections.unmodifiableList(events);
    }

    /** A listener of {@link TestEvent}s. */
    public interface TestEventListener {
        void onEvent(TestEvent e);
    }
}
