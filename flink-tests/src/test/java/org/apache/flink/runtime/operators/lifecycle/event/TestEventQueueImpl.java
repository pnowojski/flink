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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

class TestEventQueueImpl implements TestEventQueue {
    private final List<TestEvent> events = new CopyOnWriteArrayList<>();
    private final List<BlockingQueue<TestEvent>> activeEventListeners =
            new CopyOnWriteArrayList<>();

    public void add(TestEvent e) {
        events.add(e);
        activeEventListeners.forEach(l -> l.add(e));
    }

    @Override
    public void waitForEvent(Class<? extends TestEvent> eventClass) throws Exception {
        BlockingQueue<TestEvent> queue = new LinkedBlockingQueue<>();
        activeEventListeners.add(queue);
        try {
            while (eventClass.isAssignableFrom(queue.take().getClass())) {}
        } finally {
            activeEventListeners.remove(queue);
        }
    }

    @Override
    public List<TestEvent> getAll() {
        return Collections.unmodifiableList(events);
    }
}
