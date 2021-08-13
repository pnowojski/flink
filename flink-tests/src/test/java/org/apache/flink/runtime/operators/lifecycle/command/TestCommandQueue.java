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

package org.apache.flink.runtime.operators.lifecycle.command;

import org.apache.flink.testutils.junit.SharedObjects;

import java.io.Serializable;

/** A queue for {@link TestCommand} executed by operators in a test job. */
public interface TestCommandQueue extends Serializable {

    void add(TestCommand testCommand, TestCommandTarget target, TestCommandRetention retention);

    void subscribe(CommandExecutor executor, TestCommandTarget target);

    void unsubscribe(CommandExecutor executor);

    /** A target for the {@link TestCommand}. */
    interface TestCommandTarget {
        boolean matches(TestCommandTarget target);

        TestCommandTarget ALL = target -> true;

        static TestCommandTarget forOperatorId(String operatorId) {
            return new OperatorIdCommandTarget(operatorId);
        }

        class OperatorIdCommandTarget implements TestCommandTarget {
            private final String operatorId;

            public OperatorIdCommandTarget(String operatorId) {
                this.operatorId = operatorId;
            }

            @Override
            public boolean matches(TestCommandTarget target) {
                return target instanceof OperatorIdCommandTarget
                        && ((OperatorIdCommandTarget) target).operatorId.equals(operatorId);
            }
        }
    }

    /** An executor of {@link TestCommand}s. */
    interface CommandExecutor {
        void execute(TestCommand testCommand);
    }

    /** Specifies what happens when the target for the command is found. */
    enum TestCommandRetention {
        /** Remove once the command target is found, i.e. single execution. */
        REMOVE_ON_MATCH,
        /** Don't remove the command, i.e. it's executed by all currently valid targets.. */
        RETAIN_ON_MATCH
    }

    static TestCommandQueue createShared(SharedObjects shared) {
        return new SharedTestCommandQueue(shared, new TestCommandQueueImpl());
    }
}
