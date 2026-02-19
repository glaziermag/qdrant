use std::ops::Deref as _;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use itertools::Itertools as _;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio_util::task::AbortOnDropHandle;

use super::{ShardReplicaSet, clock_set};
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::replica_set::clock_set::ClockGuard;
use crate::shards::replica_set::replica_set_state::{ReplicaSetState, ReplicaState};
use crate::shards::shard::{PeerId, Shard};
use crate::shards::shard_trait::ShardOperation as _;

/// Maximum number of attempts for applying an update with a new clock.
///
/// If an update is rejected because of an old clock, we will try again with a new clock. This
/// describes the maximum number of times we try the update.
const UPDATE_MAX_CLOCK_REJECTED_RETRIES: usize = 3;

const DEFAULT_SHARD_DEACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

impl ShardReplicaSet {
    #[cfg(test)]
    fn set_ordered_write_remote_update_hook(
        &self,
        hook: Option<super::OrderedWriteRemoteUpdateHook>,
    ) {
        *self.ordered_write_remote_update_hook.write() = hook;
    }

    /// Update local shard if any without forwarding to remote shards
    ///
    /// If `force` is true, the operation will be applied unconditionally no matter the replica
    /// state. Must only be used internally.
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn update_local(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
        timeout: Option<Duration>,
        mut hw_measurement: HwMeasurementAcc,
        force: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        // `ShardOperations::update` is not guaranteed to be cancel safe, so this method is not
        // cancel safe.

        let local = self.local.read().await;

        let Some(local) = local.deref() else {
            return Ok(None);
        };

        let Some(state) = self.peer_state(self.this_peer_id()) else {
            return Ok(None);
        };

        // Don't measure hw when resharding
        if state.is_resharding() && !hw_measurement.is_disposable() {
            hw_measurement = HwMeasurementAcc::disposable();
        }

        let result = match state {
            ReplicaState::Active => {
                // Rate limit update operations on Active replica
                self.check_operation_write_rate_limiter(&hw_measurement, local, &operation)
                    .await?;
                local
                    .get()
                    .update(operation, wait, timeout, hw_measurement)
                    .await
            }

            // Force apply the operation no matter the state
            _ if force => {
                local
                    .get()
                    .update(operation, wait, timeout, hw_measurement)
                    .await
            }

            ReplicaState::Partial
            | ReplicaState::Initializing
            | ReplicaState::Resharding
            | ReplicaState::ReshardingScaleDown
            | ReplicaState::ActiveRead => {
                local
                    .get()
                    .update(operation, wait, timeout, hw_measurement)
                    .await
            }

            ReplicaState::Listener => {
                local
                    .get()
                    .update(operation, false, None, hw_measurement)
                    .await
            }

            ReplicaState::PartialSnapshot | ReplicaState::Recovery
                if operation.clock_tag.is_some_and(|tag| tag.force) =>
            {
                local
                    .get()
                    .update(operation, wait, timeout, hw_measurement)
                    .await
            }

            ReplicaState::PartialSnapshot | ReplicaState::Recovery => {
                if log::log_enabled!(log::Level::Debug) {
                    if let Some(ids) = operation.operation.point_ids() {
                        log::debug!(
                            "Operation affecting point IDs {ids:?} rejected on this peer, force flag required in recovery state",
                        );
                    } else {
                        log::debug!(
                            "Operation {operation:?} rejected on this peer, force flag required in recovery state",
                        );
                    }
                }

                return Ok(None);
            }

            ReplicaState::Dead | ReplicaState::ManualRecovery => {
                return Ok(None);
            }
        };

        result.map(Some)
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn update_with_consistency(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        timeout: Option<Duration>,
        ordering: WriteOrdering,
        update_only_existing: bool,
        mut hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        // `ShardReplicaSet::update` is not cancel safe, so this method is not cancel safe.

        let Some(leader_peer) = self.leader_peer_for_update(ordering) else {
            return Err(CollectionError::service_error(format!(
                "Cannot update shard {}:{} with {ordering:?} ordering because no leader could be selected",
                self.collection_id, self.shard_id
            )));
        };

        // Don't measure hw when resharding
        let peer_state = self.peer_state(leader_peer);
        if peer_state.is_some_and(|state| state.is_resharding()) {
            hw_measurement_acc = HwMeasurementAcc::disposable();
        }

        // If we are the leader, run the update from this replica set
        if leader_peer == self.this_peer_id() {
            self.update(
                operation,
                wait,
                timeout,
                update_only_existing,
                hw_measurement_acc,
                match ordering {
                    WriteOrdering::Strong | WriteOrdering::Medium => Some(&self.write_ordering_lock),
                    WriteOrdering::Weak => None,
                },
            )
            .await
        } else {
            // Forward the update to the designated leader
            self.forward_update(leader_peer, operation, wait, timeout, ordering, hw_measurement_acc)
                .await
                .map_err(|err| {
                    if err.is_transient() {
                        // Deactivate the peer if forwarding failed with transient error
                        let replica_state = self.replica_state.read();
                        let from_state = replica_state.get_peer_state(leader_peer);
                        self.add_locally_disabled(Some(&replica_state), leader_peer, from_state);

                        // Return service error
                        CollectionError::service_error(format!(
                            "Failed to apply update with {ordering:?} ordering via leader peer {leader_peer}: {err}"
                        ))
                    } else {
                        err
                    }
                })
        }
    }

    /// Designated a leader replica for the update based on the WriteOrdering
    fn leader_peer_for_update(&self, ordering: WriteOrdering) -> Option<PeerId> {
        match ordering {
            WriteOrdering::Weak => Some(self.this_peer_id()), // no requirement for consistency
            WriteOrdering::Medium => self.highest_alive_replica_peer_id(), // consistency with highest alive replica
            WriteOrdering::Strong => self.highest_replica_peer_id(), // consistency with highest replica
        }
    }

    fn highest_alive_replica_peer_id(&self) -> Option<PeerId> {
        let read_lock = self.replica_state.read();
        let peer_ids = read_lock.peers().keys().cloned().collect::<Vec<_>>();
        drop(read_lock);

        peer_ids
            .into_iter()
            .filter(|&peer_id| self.peer_can_be_source_of_truth(peer_id)) // re-acquire replica_state read lock
            .max()
    }

    fn highest_replica_peer_id(&self) -> Option<PeerId> {
        self.replica_state.read().peers().keys().max().cloned()
    }

    pub async fn get_clock(&self) -> ClockGuard {
        loop {
            match self.clock_set.lock().await.get_clock() {
                Some(clock) => return clock,
                // Prevent blocking async runtime with spinlock
                None => yield_now().await,
            }
        }
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        timeout: Option<Duration>,
        update_only_existing: bool,
        hw_measurement_acc: HwMeasurementAcc,
        write_ordering_lock: Option<&tokio::sync::Mutex<()>>,
    ) -> CollectionResult<UpdateResult> {
        // `ShardRepilcaSet::update_impl` is not cancel safe, so this method is not cancel safe.

        // TODO: Optimize `remotes`/`local`/`clock` locking for the "happy path"?
        //
        // E.g., refactor `update`/`update_impl`, so that it would be possible to:
        // - lock `remotes`, `local`, `clock` (in specified order!) on the *first* iteration of the loop
        // - then release and lock `remotes` and `local` *only* for all next iterations
        // - but keep initial `clock` for the whole duration of `update`
        let clock_timeout = timeout.unwrap_or(Duration::MAX);
        let mut clock = tokio::time::timeout(clock_timeout, self.get_clock())
            .await
            .map_err(|_| {
                CollectionError::timeout(
                    clock_timeout,
                    format!("Failed to acquire clock for update operation within {timeout:?}"),
                )
            })?;

        for attempt in 1..=UPDATE_MAX_CLOCK_REJECTED_RETRIES {
            let is_non_zero_tick = clock.current_tick().is_some();

            let res = self
                .update_impl(
                    operation.clone(),
                    wait,
                    timeout,
                    &mut clock,
                    update_only_existing,
                    hw_measurement_acc.clone(),
                    write_ordering_lock,
                )
                .await?;

            if let Some(res) = res {
                return Ok(res);
            }

            // Log a warning, if operation was rejected... but only if operation had a non-0 tick,
            // because operations with tick 0 should *always* be rejected and rejection is *expected*.
            if is_non_zero_tick && log::log_enabled!(log::Level::Warn) {
                if let Some(ids) = operation.point_ids() {
                    log::warn!(
                        "Operation affecting point IDs {ids:?} was rejected by some node(s), retrying... \
                         (attempt {attempt}/{UPDATE_MAX_CLOCK_REJECTED_RETRIES})"
                    );
                } else {
                    log::warn!(
                        "Operation {operation:?} was rejected by some node(s), retrying... \
                         (attempt {attempt}/{UPDATE_MAX_CLOCK_REJECTED_RETRIES})"
                    );
                }
            }
        }

        Err(CollectionError::service_error(format!(
            "Failed to apply operation {operation:?} \
             after {UPDATE_MAX_CLOCK_REJECTED_RETRIES} attempts, \
             all attempts were rejected",
        )))
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update_impl(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        timeout: Option<Duration>,
        clock: &mut clock_set::ClockGuard,
        update_only_existing: bool,
        hw_measurement_acc: HwMeasurementAcc,
        write_ordering_lock: Option<&tokio::sync::Mutex<()>>,
    ) -> CollectionResult<Option<UpdateResult>> {
        // `LocalShard::update` is not guaranteed to be cancel safe and it's impossible to cancel
        // multiple parallel updates in a way that is *guaranteed* not to introduce inconsistencies
        // between nodes, so this method is not cancel safe.

        let remotes = self.remotes.read().await;
        let local = self.local.read().await;
        let replica_count = usize::from(local.is_some()) + remotes.len();

        let this_peer_id = self.this_peer_id();

        // Target all remote peers that can receive updates
        let updatable_remote_shards: Vec<_> = remotes
            .iter()
            .filter(|rs| self.is_peer_updatable(rs.peer_id))
            .collect();

        #[cfg(test)]
        let ordered_write_remote_update_hook = self.ordered_write_remote_update_hook.read().clone();

        // Local is defined and can receive updates
        let local_is_updatable = local.is_some() && self.is_peer_updatable(this_peer_id);

        if updatable_remote_shards.is_empty() && !local_is_updatable {
            return Err(CollectionError::service_error(format!(
                "The replica set for shard {} on peer {this_peer_id} has no active replica",
                self.shard_id,
            )));
        }

        // Keep the ordering lock scope as small as possible: only serialize tick assignment/tagging.
        let write_ordering_guard = match write_ordering_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        let current_clock_tick = clock.tick_once();
        let clock_tag = ClockTag::new(this_peer_id, clock.id() as _, current_clock_tick);
        let operation = OperationWithClockTag::new(operation, Some(clock_tag));
        drop(write_ordering_guard);

        let mut update_futures = Vec::with_capacity(updatable_remote_shards.len() + 1);

        if let Some(local) = local.deref()
            && self.is_peer_updatable(this_peer_id)
        {
            let local_wait = if self.peer_state(this_peer_id) == Some(ReplicaState::Listener) {
                false
            } else {
                wait
            };

            if self.peer_is_active(this_peer_id) {
                // Check write rate limiter before proceeding if replica active
                self.check_operation_write_rate_limiter(&hw_measurement_acc, local, &operation)
                    .await?;
            }

            let operation = operation.clone();

            let hw_acc = hw_measurement_acc.clone();
            let local_update = async move {
                local
                    .get()
                    .update(operation, local_wait, timeout, hw_acc)
                    .await
                    .map(|ok| (this_peer_id, ok))
                    .map_err(|err| (this_peer_id, err))
            };

            update_futures.push(local_update.left_future());
        }

        for remote in updatable_remote_shards {
            let operation = operation.clone();

            let hw_acc = hw_measurement_acc.clone();
            #[cfg(test)]
            let ordered_write_remote_update_hook = ordered_write_remote_update_hook.clone();
            let remote_update = async move {
                #[cfg(test)]
                if let Some(test_remote_update_hook) = ordered_write_remote_update_hook {
                    return test_remote_update_hook(
                        remote.peer_id,
                        operation,
                        wait,
                        timeout,
                        hw_acc,
                    )
                    .await
                    .map(|ok| (remote.peer_id, ok))
                    .map_err(|err| (remote.peer_id, err));
                }

                remote
                    .update(operation, wait, timeout, hw_acc)
                    .await
                    .map(|ok| (remote.peer_id, ok))
                    .map_err(|err| (remote.peer_id, err))
            };

            update_futures.push(remote_update.right_future());
        }

        let all_res: Vec<Result<_, _>> = match self.shared_storage_config.update_concurrency {
            Some(concurrency) => {
                futures::stream::iter(update_futures)
                    .buffer_unordered(concurrency.get())
                    .collect()
                    .await
            }

            None => FuturesUnordered::from_iter(update_futures).collect().await,
        };

        drop(local);
        drop(remotes);

        let write_consistency_factor = self
            .collection_config
            .read()
            .await
            .params
            .write_consistency_factor
            .get() as usize;

        let minimal_success_count = write_consistency_factor.min(replica_count);

        let (successes, failures): (Vec<_>, Vec<_>) = all_res.into_iter().partition_result();

        // Advance clock if some replica echoed *newer* tick

        let new_clock_tick = successes
            .iter()
            .filter_map(|(_, result)| {
                let echo_tag = result.clock_tag?;

                if echo_tag.peer_id != clock_tag.peer_id {
                    debug_assert!(
                        false,
                        "Echoed clock tag peer_id does not match the original",
                    );
                    return None;
                }

                if echo_tag.clock_id != clock_tag.clock_id {
                    debug_assert!(
                        false,
                        "Echoed clock tag clock_id does not match the original",
                    );
                    return None;
                }

                Some(echo_tag.clock_tick)
            })
            .max();

        if let Some(new_clock_tick) = new_clock_tick {
            clock.advance_to(new_clock_tick);
        }

        // Notify consensus about replica failures if:
        // 1. there are some failures, but enough successes for the operation to be accepted
        // 2. a resharding replica failed, and there are not enough successes for the operation to be accepted
        //
        // Notify user about potential consistency problems if:
        // 1. there are some failures and enough successes, but we fail to deactivate the failed replicas
        // 2. successes were not applied to any Active or Resharding replica
        //
        // Notify user with operation error if:
        // 1. there are not enough successes for the operation to be accepted

        let failure_error = if let Some((peer_id, collection_error)) = failures.first() {
            format!("Failed peer: {peer_id}, error: {collection_error}")
        } else {
            String::new()
        };

        if !failures.is_empty() {
            for (peer_id, err) in &failures {
                log::warn!(
                    "Failed to update shard {}:{} on peer {peer_id}, error: {err}",
                    self.collection_id,
                    self.shard_id,
                );
            }

            // If there is at least one full-complete operation, we can't ignore non-transient errors (4xx)
            // And we must deactivate failed replicas to ensure consistency
            let has_full_completed_updates = successes.iter().any(|(_, res)| match res.status {
                UpdateStatus::Completed => true,
                UpdateStatus::Acknowledged => false,
                UpdateStatus::ClockRejected => false,
                UpdateStatus::WaitTimeout => false,
            });

            if successes.len() >= minimal_success_count {
                // If there are enough successes, deactivate failed replicas
                // Failed replicas will automatically recover from another replica ensuring consistency

                let failures_to_handle: Vec<_> = if !has_full_completed_updates {
                    // We can only deactivate transient errors
                    failures
                        .into_iter()
                        .filter(|(_, err)| err.is_transient())
                        .collect()
                } else {
                    failures
                };

                let wait_for_deactivation = self.handle_failed_replicas(
                    &failures_to_handle,
                    &self.replica_state.read(),
                    update_only_existing,
                );

                // Wait for replica failures to be accepted, otherwise return consistency error
                if wait && wait_for_deactivation {
                    // ToDo: allow timeout configuration in API
                    let timeout = DEFAULT_SHARD_DEACTIVATION_TIMEOUT;

                    let replica_state = self.replica_state.clone();
                    let peer_ids: Vec<_> = failures_to_handle
                        .iter()
                        .map(|(peer_id, _)| *peer_id)
                        .collect();

                    let shards_disabled =
                        AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                            replica_state.wait_for(
                                |state| {
                                    peer_ids.iter().all(|peer_id| {
                                        // Not found means that peer is dead

                                        // Wait for replica deactivation.
                                        let is_active = state
                                            .peers()
                                            .get(peer_id)
                                            .map(|state| state.can_be_source_of_truth())
                                            .unwrap_or(false);

                                        !is_active
                                    })
                                },
                                timeout,
                            )
                        }))
                        .await?;

                    if !shards_disabled {
                        return Err(CollectionError::service_error(format!(
                            "Some replica of shard {} failed to apply operation and deactivation \
                            timed out after {timeout:?}. Consistency of this update is not guaranteed. Please retry. {failure_error}",
                            self.shard_id,
                        )));
                    }
                }
            } else {
                // If there aren't enough successes, report error to user

                // TODO(resharding): reconsider how we count/deactivate resharding replicas.
                self.handle_failed_replicas(
                    failures
                        .iter()
                        .filter(|(peer_id, _)| self.peer_is_resharding(*peer_id)),
                    &self.replica_state.read(),
                    update_only_existing,
                );

                let (_peer_id, err) = failures.into_iter().next().unwrap();
                return Err(err);
            }
        }

        // Successes must have applied to at least one active replica
        if !successes
            .iter()
            .any(|&(peer_id, _)| self.peer_can_be_source_of_truth(peer_id))
        {
            return Err(CollectionError::service_error(format!(
                "Failed to apply operation to at least one `Active` replica. \
                 Consistency of this update is not guaranteed. Please retry. {failure_error}",
            )));
        }

        let is_any_operation_rejected = successes
            .iter()
            .any(|(_, res)| matches!(res.status, UpdateStatus::ClockRejected));

        if is_any_operation_rejected {
            return Ok(None);
        }

        let res = Self::merge_successful_update_results(&successes);

        Ok(Some(res))
    }

    /// Check write rate limiter for the operation
    ///
    /// Lazily compute the cost of the operation and check against the write rate limiter
    async fn check_operation_write_rate_limiter(
        &self,
        hw_measurement: &HwMeasurementAcc,
        local: &Shard,
        operation: &OperationWithClockTag,
    ) -> CollectionResult<()> {
        self.check_write_rate_limiter(hw_measurement, || async {
            let mut ratelimiter_cost = 1;

            // Estimate the cost based on affected points if filter is available.
            match local
                .estimate_request_cardinality(&operation.operation, hw_measurement)
                .await
            {
                Ok(est) => ratelimiter_cost = 1.max(est.exp),
                Err(err) => log::error!("Estimating cardinality: {err:?}"),
            }

            ratelimiter_cost
        })
        .await?;
        Ok(())
    }

    /// Whether to send updates to the given peer
    ///
    /// A peer in dead state, or a locally disabled peer, will not accept updates.
    fn is_peer_updatable(&self, peer_id: PeerId) -> bool {
        let Some(state) = self.peer_state(peer_id) else {
            return false;
        };

        state.is_updatable() && !self.is_locally_disabled(peer_id)
    }

    fn peer_is_resharding(&self, peer_id: PeerId) -> bool {
        let is_resharding = matches!(
            self.peer_state(peer_id),
            Some(ReplicaState::Resharding | ReplicaState::ReshardingScaleDown)
        );

        is_resharding && !self.is_locally_disabled(peer_id)
    }

    fn handle_failed_replicas<'a>(
        &self,
        failures: impl IntoIterator<Item = &'a (PeerId, CollectionError)>,
        state: &ReplicaSetState,
        update_only_existing: bool,
    ) -> bool {
        let mut wait_for_deactivation = false;

        for (peer_id, err) in failures {
            let Some(peer_state) = state.get_peer_state(*peer_id) else {
                continue;
            };

            // Ignore errors entirely for dead and listener replicas
            match peer_state {
                ReplicaState::Dead | ReplicaState::Listener | ReplicaState::ManualRecovery => {
                    continue;
                }
                ReplicaState::Active
                | ReplicaState::Initializing
                | ReplicaState::Partial
                | ReplicaState::Recovery
                | ReplicaState::PartialSnapshot
                | ReplicaState::Resharding
                | ReplicaState::ReshardingScaleDown
                | ReplicaState::ActiveRead => (),
            }

            // Handle a special case where transfer receiver is not in the expected replica state yet.
            // Data consistency will be handled by the shard transfer and the associated proxies.
            if peer_state.is_partial_or_recovery() && err.is_pre_condition_failed() {
                continue;
            }

            // Ignore missing point errors if replica is in partial or recovery state
            // Partial or recovery state indicates that the replica is receiving a shard transfer,
            // it might not have received all the points yet
            // See: <https://github.com/qdrant/qdrant/pull/5991>
            if peer_state.is_partial_or_recovery() && err.is_missing_point() {
                continue;
            }

            if update_only_existing && err.is_missing_point() {
                continue;
            }

            if err.is_transient() || peer_state == ReplicaState::Initializing {
                // If the error is transient, we should not deactivate the peer
                // before allowing other operations to continue.
                // Otherwise, the failed node can become responsive again, before
                // the other nodes deactivate it, so the storage might be inconsistent.
                wait_for_deactivation = true;
            }

            log::debug!(
                "Deactivating peer {peer_id} because of failed update of shard {}:{}",
                self.collection_id,
                self.shard_id,
            );

            // Deactivate replica in consensus if it matches the state we expect
            // Always deactivate the replica if its in a shard transfer related state
            let from_state = Some(peer_state).filter(|state| !state.is_partial_or_recovery());

            self.add_locally_disabled(Some(state), *peer_id, from_state);
        }

        wait_for_deactivation
    }

    /// Forward update to the leader replica
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn forward_update(
        &self,
        leader_peer: PeerId,
        operation: CollectionUpdateOperations,
        wait: bool,
        timeout: Option<Duration>,
        ordering: WriteOrdering,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        // `RemoteShard::forward_update` is cancel safe, so this method is cancel safe.

        let remotes_guard = self.remotes.read().await;

        let Some(remote_leader) = remotes_guard.iter().find(|r| r.peer_id == leader_peer) else {
            return Err(CollectionError::service_error(format!(
                "Cannot forward update to shard {} because was removed from the replica set",
                self.shard_id
            )));
        };

        remote_leader
            .forward_update(
                OperationWithClockTag::from(operation),
                wait,
                timeout,
                ordering,
                hw_measurement_acc,
            ) // `clock_tag` *has to* be `None`!
            .await
    }

    /// Pick a successful update result to return from a replica set.
    ///
    /// We pick the reply from the highest peer ID. This makes the returned response deterministic.
    fn merge_successful_update_results(successes: &[(PeerId, UpdateResult)]) -> UpdateResult {
        debug_assert!(!successes.is_empty());
        debug_assert!(
            !successes
                .iter()
                .any(|(_, r)| r.status == UpdateStatus::ClockRejected),
            "ClockRejected must be handled before merging successful results",
        );

        // Aggregate status: WaitTimeout > .. > ClockRejected
        let status = successes
            .iter()
            .map(|(_, res)| res.status)
            .max_by_key(|s| s.priority())
            .unwrap_or(UpdateStatus::Acknowledged);

        let mut result = successes
            .iter()
            .max_by_key(|(peer_id, _)| *peer_id)
            .map(|(_, res)| *res)
            .expect("successes is not empty");

        result.status = status;
        result
    }

    /// Send plunger operation
    ///
    /// Returns oneshot channel receiver that will be notified once the plunger operation is
    /// processed. Returns `None` if local shard is not present.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn plunge_local_async(&self) -> CollectionResult<Option<oneshot::Receiver<()>>> {
        match self.local.read().await.deref() {
            Some(local) => local.plunge_async().await.map(Some),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use common::budget::ResourceBudget;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::save_on_disk::SaveOnDisk;
    use segment::types::Distance;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Handle;
    use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

    use super::*;
    use crate::config::*;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::replica_set::{
        AbortShardTransfer, ChangePeerFromState, OrderedWriteRemoteUpdateHook,
    };
    use crate::tests::fixtures::delete_point_operation;

    #[test]
    fn test_merge_successful_update_results_wait_timeout_dominates() {
        let this_peer_id: PeerId = 1;

        let local_tag = ClockTag::new_with_token(this_peer_id, 7, 10, 0);
        let remote_tag = ClockTag::new_with_token(this_peer_id, 7, 12, 0);

        let successes = vec![
            (
                this_peer_id,
                UpdateResult {
                    operation_id: Some(10),
                    status: UpdateStatus::Completed,
                    clock_tag: Some(local_tag),
                },
            ),
            (
                2,
                UpdateResult {
                    operation_id: Some(20),
                    status: UpdateStatus::WaitTimeout,
                    clock_tag: Some(remote_tag),
                },
            ),
        ];

        let merged = ShardReplicaSet::merge_successful_update_results(&successes);

        assert_eq!(merged.status, UpdateStatus::WaitTimeout);
        assert_eq!(merged.operation_id, Some(20));
        assert_eq!(merged.clock_tag.unwrap().clock_tick, 12);
    }

    #[test]
    fn test_merge_successful_update_results_prefers_highest_peer_id() {
        let this_peer_id: PeerId = 1;

        let local_tag = ClockTag::new_with_token(this_peer_id, 7, 10, 0);
        let remote_tag = ClockTag::new_with_token(this_peer_id, 7, 11, 0);

        let successes = vec![
            (
                this_peer_id,
                UpdateResult {
                    operation_id: Some(10),
                    status: UpdateStatus::Acknowledged,
                    clock_tag: Some(local_tag),
                },
            ),
            (
                2,
                UpdateResult {
                    operation_id: Some(20),
                    status: UpdateStatus::Completed,
                    clock_tag: Some(remote_tag),
                },
            ),
        ];

        let merged = ShardReplicaSet::merge_successful_update_results(&successes);

        assert_eq!(merged.status, UpdateStatus::Completed);
        assert_eq!(merged.operation_id, Some(20));
        assert_eq!(merged.clock_tag.unwrap().clock_tick, 11);
    }

    #[tokio::test]
    async fn test_highest_replica_peer_id() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let rs = new_shard_replica_set(&collection_dir).await;

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        // at build time the replicas are all dead, they need to be activated
        assert_eq!(rs.highest_alive_replica_peer_id(), None);

        rs.set_replica_state(1, ReplicaState::Active).await.unwrap();
        rs.set_replica_state(3, ReplicaState::Active).await.unwrap();
        rs.set_replica_state(4, ReplicaState::Active).await.unwrap();
        rs.set_replica_state(5, ReplicaState::Partial)
            .await
            .unwrap();

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        assert_eq!(rs.highest_alive_replica_peer_id(), Some(4));
    }

    #[tokio::test]
    async fn test_strong_writes_do_not_serialize_across_remote_await_barrier() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

        let replica_set = Arc::new(
            new_shard_replica_set_with(
                &collection_dir,
                2,
                true,
                HashSet::from([1]),
                NonZeroU32::new(1).unwrap(),
            )
            .await,
        );

        replica_set
            .set_replica_state(2, ReplicaState::Recovery)
            .await
            .unwrap();
        replica_set
            .set_replica_state(1, ReplicaState::Active)
            .await
            .unwrap();

        const N: usize = 8;
        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let started = Arc::new(AtomicUsize::new(0));

        let hook: OrderedWriteRemoteUpdateHook = {
            let barrier = barrier.clone();
            let started = started.clone();
            Arc::new(
                move |_peer_id, operation, _wait, _timeout, _hw_measurement| {
                    let barrier = barrier.clone();
                    let started = started.clone();
                    Box::pin(async move {
                        let tag = operation
                            .clock_tag
                            .expect("ordered write operation must have a tag");

                        started.fetch_add(1, AtomicOrdering::SeqCst);
                        barrier.wait().await;

                        Ok(UpdateResult {
                            operation_id: Some(tag.clock_tick),
                            status: UpdateStatus::Completed,
                            clock_tag: Some(tag),
                        })
                    })
                },
            )
        };
        replica_set.set_ordered_write_remote_update_hook(Some(hook));

        let mut tasks = Vec::with_capacity(N);
        for i in 0..N {
            let rs = replica_set.clone();
            tasks.push(tokio::spawn(async move {
                rs.update_with_consistency(
                    delete_point_operation(i as u64),
                    true,
                    None,
                    WriteOrdering::Strong,
                    false,
                    HwMeasurementAcc::new(),
                )
                .await
            }));
        }

        tokio::time::timeout(Duration::from_secs(3), async {
            for t in tasks {
                t.await.unwrap().unwrap();
            }
        })
        .await
        .expect("writes must not be serialized by write_ordering_lock across remote await");

        replica_set.set_ordered_write_remote_update_hook(None);

        assert_eq!(
            started.load(AtomicOrdering::SeqCst),
            N,
            "all {N} writes should have reached the remote await point"
        );
    }

    #[tokio::test]
    async fn test_write_ordering_lock_not_held_while_remote_update_is_blocked() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

        let replica_set = Arc::new(
            new_shard_replica_set_with(
                &collection_dir,
                2,
                true,
                HashSet::from([1]),
                NonZeroU32::new(1).unwrap(),
            )
            .await,
        );

        replica_set
            .set_replica_state(2, ReplicaState::Recovery)
            .await
            .unwrap();
        replica_set
            .set_replica_state(1, ReplicaState::Active)
            .await
            .unwrap();

        let (remote_started_tx, mut remote_started_rx) = mpsc::unbounded_channel::<u64>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release_rx = Arc::new(Mutex::new(Some(release_rx)));

        let hook: OrderedWriteRemoteUpdateHook = {
            let release_rx = release_rx.clone();
            Arc::new(
                move |_peer_id, operation, _wait, _timeout, _hw_measurement| {
                    let release_rx = release_rx.clone();
                    let remote_started_tx = remote_started_tx.clone();
                    Box::pin(async move {
                        let tag = operation
                            .clock_tag
                            .expect("ordered write operation must have a tag");
                        remote_started_tx
                            .send(tag.clock_tick)
                            .expect("must record remote start");

                        let rx = release_rx
                            .lock()
                            .await
                            .take()
                            .expect("release channel must be available");
                        let _ = rx.await;

                        Ok(UpdateResult {
                            operation_id: Some(tag.clock_tick),
                            status: UpdateStatus::Completed,
                            clock_tag: Some(tag),
                        })
                    })
                },
            )
        };

        replica_set.set_ordered_write_remote_update_hook(Some(hook));

        let rs_task = replica_set.clone();
        let task = tokio::spawn(async move {
            rs_task
                .update_with_consistency(
                    delete_point_operation(123),
                    true,
                    None,
                    WriteOrdering::Strong,
                    false,
                    HwMeasurementAcc::new(),
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), remote_started_rx.recv())
            .await
            .expect("remote must start")
            .expect("channel must yield tick");

        let lock_guard = tokio::time::timeout(
            Duration::from_millis(200),
            replica_set.write_ordering_lock.lock(),
        )
        .await
        .expect("write_ordering_lock should be free while remote update is awaiting");
        drop(lock_guard);

        release_tx.send(()).unwrap();
        task.await.unwrap().unwrap();

        replica_set.set_ordered_write_remote_update_hook(None);
    }

    #[tokio::test]
    #[ignore = "manual perf smoke test; run locally to compare before/after #8094"]
    async fn perf_smoke_8094_ordered_writes_overlap_remote_rtt() {
        let n: usize = std::env::var("N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16);
        let delay_ms: u64 = std::env::var("DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);

        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let replica_set = Arc::new(
            new_shard_replica_set_with(
                &collection_dir,
                2,
                true,
                HashSet::from([1]),
                NonZeroU32::new(1).unwrap(),
            )
            .await,
        );
        replica_set
            .set_replica_state(2, ReplicaState::Recovery)
            .await
            .unwrap();
        replica_set
            .set_replica_state(1, ReplicaState::Active)
            .await
            .unwrap();

        let start_barrier = Arc::new(tokio::sync::Barrier::new(n + 1));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let hook: OrderedWriteRemoteUpdateHook = {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            Arc::new(
                move |_peer_id, operation, _wait, _timeout, _hw_measurement| {
                    let in_flight = in_flight.clone();
                    let max_in_flight = max_in_flight.clone();
                    Box::pin(async move {
                        let cur = in_flight.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        max_in_flight.fetch_max(cur, AtomicOrdering::SeqCst);

                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                        in_flight.fetch_sub(1, AtomicOrdering::SeqCst);

                        let tag = operation
                            .clock_tag
                            .expect("ordered write operation must have a tag");
                        Ok(UpdateResult {
                            operation_id: Some(tag.clock_tick),
                            status: UpdateStatus::Completed,
                            clock_tag: Some(tag),
                        })
                    })
                },
            )
        };
        replica_set.set_ordered_write_remote_update_hook(Some(hook));

        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let rs = replica_set.clone();
            let b = start_barrier.clone();
            tasks.push(tokio::spawn(async move {
                b.wait().await;
                rs.update_with_consistency(
                    delete_point_operation(i as u64),
                    true,
                    None,
                    WriteOrdering::Strong,
                    false,
                    HwMeasurementAcc::new(),
                )
                .await
                .expect("ordered write should succeed");
            }));
        }

        let started = Instant::now();
        start_barrier.wait().await;

        for t in tasks {
            t.await.unwrap();
        }
        let elapsed = started.elapsed();

        replica_set.set_ordered_write_remote_update_hook(None);

        let max = max_in_flight.load(AtomicOrdering::SeqCst);
        eprintln!(
            "[perf_smoke_8094] N={n}, DELAY_MS={delay_ms}, elapsed={elapsed:?}, max_in_flight_remote={max}, approx_ops_per_sec={:.1}",
            (n as f64) / elapsed.as_secs_f64()
        );

        assert!(
            max > 1,
            "expected >1 in-flight remote updates; got {max}. If this is on a very constrained runtime, try smaller N or larger DELAY_MS."
        );
    }

    #[tokio::test]
    #[ignore = "nondeterministic stress test; run locally to shake out regressions around #8094"]
    async fn chaos_8094_random_remote_jitter_many_concurrent_ordered_writes() {
        let n: usize = std::env::var("N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);
        let max_delay_ms: u64 = std::env::var("MAX_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(40);
        let overall_timeout_s: u64 = std::env::var("TIMEOUT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let seed: u64 = std::env::var("SEED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });

        eprintln!("[chaos_8094] seed={seed} N={n} MAX_DELAY_MS={max_delay_ms} TIMEOUT_S={overall_timeout_s}");

        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let replica_set = Arc::new(
            new_shard_replica_set_with(
                &collection_dir,
                2,
                true,
                HashSet::from([1]),
                NonZeroU32::new(1).unwrap(),
            )
            .await,
        );
        replica_set
            .set_replica_state(2, ReplicaState::Recovery)
            .await
            .unwrap();
        replica_set
            .set_replica_state(1, ReplicaState::Active)
            .await
            .unwrap();

        let start_barrier = Arc::new(tokio::sync::Barrier::new(n + 1));
        let seq = Arc::new(AtomicU64::new(seed));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let hook: OrderedWriteRemoteUpdateHook = {
            let seq = seq.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            Arc::new(
                move |_peer_id, operation, _wait, _timeout, _hw_measurement| {
                    let seq = seq.clone();
                    let in_flight = in_flight.clone();
                    let max_in_flight = max_in_flight.clone();
                    Box::pin(async move {
                        let cur = in_flight.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        max_in_flight.fetch_max(cur, AtomicOrdering::SeqCst);

                        let s = seq.fetch_add(0x9E3779B97F4A7C15, AtomicOrdering::SeqCst);
                        let jitter = (s ^ (s >> 33)).wrapping_mul(0xff51afd7ed558ccd);
                        let delay = jitter % max_delay_ms.max(1);

                        tokio::time::sleep(Duration::from_millis(delay)).await;

                        in_flight.fetch_sub(1, AtomicOrdering::SeqCst);

                        let tag = operation
                            .clock_tag
                            .expect("ordered write operation must have a tag");
                        Ok(UpdateResult {
                            operation_id: Some(tag.clock_tick),
                            status: UpdateStatus::Completed,
                            clock_tag: Some(tag),
                        })
                    })
                },
            )
        };
        replica_set.set_ordered_write_remote_update_hook(Some(hook));

        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let rs = replica_set.clone();
            let b = start_barrier.clone();
            tasks.push(tokio::spawn(async move {
                b.wait().await;
                rs.update_with_consistency(
                    delete_point_operation(i as u64),
                    true,
                    None,
                    WriteOrdering::Strong,
                    false,
                    HwMeasurementAcc::new(),
                )
                .await
            }));
        }

        let started = Instant::now();
        start_barrier.wait().await;

        let res = tokio::time::timeout(Duration::from_secs(overall_timeout_s), async {
            for t in tasks {
                t.await.unwrap().expect("ordered write should succeed");
            }
        })
        .await;

        let elapsed = started.elapsed();
        replica_set.set_ordered_write_remote_update_hook(None);

        let max = max_in_flight.load(AtomicOrdering::SeqCst);
        eprintln!(
            "[chaos_8094] elapsed={elapsed:?} max_in_flight_remote={max} approx_ops_per_sec={:.1}",
            (n as f64) / elapsed.as_secs_f64()
        );

        res.expect(
            "chaos run timed out (possible regression to serialized remote awaits / deadlock)"
        );
    }

    const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 2,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: Some(2),
        prevent_unoptimized: None,
    };

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        new_shard_replica_set_with(
            collection_dir,
            1,
            false,
            HashSet::from([2, 3, 4, 5]),
            NonZeroU32::new(2).unwrap(),
        )
        .await
    }

    async fn new_shard_replica_set_with(
        collection_dir: &TempDir,
        this_peer_id: PeerId,
        local: bool,
        remotes: HashSet<PeerId>,
        write_consistency_factor: NonZeroU32,
    ) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = Handle::current();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
            shard_number: NonZeroU32::new(4).unwrap(),
            replication_factor: NonZeroU32::new(3).unwrap(),
            write_consistency_factor,
            ..CollectionParams::empty()
        };

        let config = CollectionConfigInternal {
            params: collection_params,
            optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: None,
            strict_mode_config: None,
            uuid: None,
            metadata: None,
        };

        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let shared_config = Arc::new(RwLock::new(config.clone()));
        ShardReplicaSet::build(
            1,
            None,
            "test_collection".to_string(),
            this_peer_id,
            local,
            remotes,
            dummy_on_replica_failure(),
            dummy_abort_shard_transfer(),
            collection_dir.path(),
            shared_config,
            config.optimizer_config.clone(),
            Default::default(),
            payload_index_schema,
            Default::default(),
            update_runtime,
            search_runtime,
            ResourceBudget::default(),
            None,
        )
        .await
        .unwrap()
    }

    fn dummy_on_replica_failure() -> ChangePeerFromState {
        Arc::new(move |_peer_id, _shard_id, _from_state| {})
    }

    fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_shard_transfer, _reason| {})
    }
}
