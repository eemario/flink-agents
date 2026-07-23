/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.operator.parallel;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Coordinates worker execution of {@link ParallelExecutionTask}: callers {@link #addTask(Object)
 * add} one node per queued task, and the coordinator submits one anonymous <em>dispatch permit</em>
 * per node to an elastic {@link ThreadPoolExecutor}. A pool thread redeems a permit under the lock
 * (node poll and FIFO task pull share the lock hold, keeping their binding correct — permits never
 * name a task); commits always run on the mailbox thread in per-key taskIndex order.
 */
public final class ParallelExecutionCoordinator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelExecutionCoordinator.class);

    private static final long WORKER_TERMINATION_TIMEOUT_SECONDS = 180L;

    @FunctionalInterface
    public interface MailboxDispatcher {
        void execute(ThrowingRunnable<? extends Exception> command, String description);
    }

    private final ParallelExecutionTaskQueue schedule;
    private final ParallelExecutionLock parallelExecutionLock;
    private final ThreadPoolExecutor workerExecutor;
    private final MailboxDispatcher mailboxDispatcher;

    /** Supplies a fresh work; the work pulls and prepares its own task inside {@code execute()}. */
    private final Supplier<ParallelExecutionTask> workFactory;

    /**
     * Permits submitted and not yet fully redeemed; compared against the pool size by {@link
     * DispatchQueue#offer} to decide when the pool must grow.
     */
    private final AtomicInteger submittedPermits = new AtomicInteger();

    /**
     * Creates a coordinator with an elastic worker pool: one resident thread, on-demand growth up
     * to {@code maxWorkers}, and idle retirement after {@code workerIdleTimeoutMillis}. Admission
     * is not gated here; the operator bounds outstanding work via input-record backpressure.
     */
    public ParallelExecutionCoordinator(
            ParallelExecutionLock parallelExecutionLock,
            MailboxDispatcher mailboxDispatcher,
            Supplier<ParallelExecutionTask> workFactory,
            int maxWorkers,
            long workerIdleTimeoutMillis) {
        this.parallelExecutionLock = checkNotNull(parallelExecutionLock);
        this.schedule = new ParallelExecutionTaskQueue(parallelExecutionLock);
        this.mailboxDispatcher = checkNotNull(mailboxDispatcher);
        this.workFactory = checkNotNull(workFactory);
        DispatchQueue dispatchQueue = new DispatchQueue();
        this.workerExecutor =
                new ThreadPoolExecutor(
                        1,
                        maxWorkers,
                        workerIdleTimeoutMillis,
                        TimeUnit.MILLISECONDS,
                        dispatchQueue,
                        (permit, pool) -> {
                            // Only reachable when offer() returned false to grow the pool but the
                            // addWorker race was lost (already at max), or during shutdown.
                            if (pool.isShutdown()) {
                                submittedPermits.decrementAndGet();
                            } else {
                                dispatchQueue.forceOffer(permit);
                            }
                        });
    }

    /**
     * Adds one task node for {@code key} and submits one anonymous dispatch permit (a permit means
     * "pull one node under the lock", never a specific task, so pool threads may redeem permits in
     * any order). Must be called on the mailbox thread holding the lock.
     */
    public void addTask(Object key) {
        schedule.addTask(key);
        submittedPermits.incrementAndGet();
        workerExecutor.execute(this::runOneTask);
    }

    /** Whether {@code key} still has outstanding (added but not yet committed) work. */
    public boolean hasOutstanding(Object key) {
        return schedule.hasOutstanding(key);
    }

    /**
     * Redeems one dispatch permit: under the lock, takes the highest-priority pending node, runs a
     * fresh work, then hands the completion to the mailbox commit drain.
     */
    private void runOneTask() {
        try {
            // None of the interruptible calls below holds the lock when it throws, so one handler
            // treats every interrupt as the shutdown signal.
            try {
                // Fresh pulls acquire at the lowest priority: async resumes always take precedence.
                parallelExecutionLock.acquireByWorker(Long.MAX_VALUE, Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            // Holding the lock from here on: every failure is funneled to the mailbox (an uncaught
            // throwable would only kill this pool thread silently) and the lock is always returned.
            try {
                ParallelExecutionTaskQueue.Node node = schedule.pollPendingNode();
                ParallelExecutionTask work = workFactory.get();
                work.setup(node.getKey(), node.getRecordIndex(), node.getTaskIndex());
                node.setWork(work);
                // execute() never throws: it captures pull/prepare/run failures and rethrows at
                // commit.
                work.execute();
                schedule.markDone(node);
                mailboxDispatcher.execute(() -> completeOnMailbox(node), "complete action task");
            } catch (Throwable t) {
                mailboxDispatcher.execute(
                        () -> {
                            throw t;
                        },
                        "throw action task scheduling failure");
            } finally {
                parallelExecutionLock.releaseToMain();
            }
        } finally {
            submittedPermits.decrementAndGet();
        }
    }

    /**
     * Mailbox-thread commit drain: commits the completed node's key group in taskIndex order,
     * stopping at the first not-yet-done head (picked up by a later drain), then runs the group's
     * follow-up.
     */
    private void completeOnMailbox(ParallelExecutionTaskQueue.Node completedNode) throws Exception {
        parallelExecutionLock.acquireByMain();
        try {
            Object key = completedNode.getKey();
            ParallelExecutionTask lastCommitted = null;
            while (true) {
                ParallelExecutionTaskQueue.Node head = schedule.pollCommittableHead(key);
                if (head == null) {
                    break;
                }
                try {
                    head.getWork().restoreContext();
                    head.getWork().commit();
                    lastCommitted = head.getWork();
                } finally {
                    schedule.removeCommitted(head);
                }
            }
            completedNode.getWork().finishGroup(lastCommitted);
        } finally {
            parallelExecutionLock.release();
        }
    }

    @Override
    public void close() {
        // Wait for in-flight workers to terminate before the caller releases resources they may
        // still be using. Interrupts are re-issued each round: a worker may block on the lock
        // (e.g. a sticky main-grant nobody consumes while the mailbox waits here) only AFTER the
        // first interrupt was delivered, so a single shutdownNow is not enough. Bounded: workers
        // running user code may ignore the interrupt.
        long deadlineNanos =
                System.nanoTime() + TimeUnit.SECONDS.toNanos(WORKER_TERMINATION_TIMEOUT_SECONDS);
        try {
            do {
                workerExecutor.shutdownNow();
            } while (!workerExecutor.awaitTermination(1, TimeUnit.SECONDS)
                    && System.nanoTime() < deadlineNanos);
            if (!workerExecutor.isTerminated()) {
                LOG.warn(
                        "Worker pool did not terminate within {}s; proceeding with close.",
                        WORKER_TERMINATION_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @VisibleForTesting
    int getWorkerPoolSizeForTesting() {
        return workerExecutor.getPoolSize();
    }

    /**
     * Work queue of anonymous dispatch permits. {@link #offer} reports "full" when every pool
     * thread is busy and the pool may still grow — the only way to make {@link ThreadPoolExecutor}
     * add threads beyond its core size; the refused permit is handed to the new thread or re-queued
     * by the rejection handler. Idle threads simply time out polling this queue and exit.
     */
    private final class DispatchQueue extends LinkedBlockingQueue<Runnable> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean offer(Runnable permit) {
            ThreadPoolExecutor pool = workerExecutor;
            if (pool != null) {
                int poolSize = pool.getPoolSize();
                if (poolSize < pool.getMaximumPoolSize() && submittedPermits.get() > poolSize) {
                    // Every thread is busy and the pool is below its cap: refuse, forcing the
                    // pool to spawn a thread for this permit.
                    return false;
                }
            }
            return super.offer(permit);
        }

        private boolean forceOffer(Runnable permit) {
            return super.offer(permit);
        }
    }
}
