const annWasEverBound = "pv.kubernetes.io/bound-completed"
const annBoundByController = "pv.kubernetes.io/bound-by-controller"

// This must be async-safe, idempotent, and crash/restart safe, since it
// happens in a loop as well as on-demand.
func SyncPVC(pvc *PVClaim) {
	if pvc.Annotations[annWasEverBound] == "" {
		// This is a new PVC that has not completed binding
		// OBSERVATION: pvc is "Pending"
		if pvc.Spec.VolumePtr == nil {
			// User did not care which PV they get
			pv = FindAcceptablePV(pvc)
			if pv == nil {
				// No PV could be found
				// OBSERVATION: pvc is "Pending", will retry
				return
			} else /* pv != nil */ {
				// Found a PV for this claim
				// OBSERVATION: pvc is "Pending", pv is "Available"
				if pv.Spec.ClaimPtr == nil {
					pv.Spec.ClaimPtr = pvc
					pv.Spec.ClaimPtr.UID = pvc.UID
					pv.Annotations[annBoundByController] = "yes"
				}
				// NOTE: do not set 'everBound' annotation here. It must be
				// set after both the PV and PVC are updated; additionally, we
				// can use the bound-by-controller annotation on the PV to
				// distinguish between a valid state and a broken one
				//
				// NOTE (later): we decided that we will not, in fact, ever
				// set the 'ever bound' annotation on the PV in order to
				// reduce the number of places that annotations are set, in
				// general
				//
				// NOTE (even later): it is possible that the PV has already
				// been committed; FOR NOW, we will do a second commit with
				// the same information, leaving as a possible future
				// optimization to check the state of the PV and avoid a
				// second, NOP commit
				pv.Status.Phase = Bound
				if err := CommitPV(pv); err != nil {
					// Nothing was saved; we will fall back into the same
					// condition in the next call to this method
					return
				}
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				pvc.Spec.VolumePtr = pv
				pvc.Annotations[annWasEverBound] = "yes"
				pvc.Annotations[annBoundByController] = "yes"
				pvc.Status.Phase = Bound
				if err := CommitPVC(pvc); err != nil {
					// Commit failed; we will handle this partially committed
					// state in the next call to syncPVC
					return
				}
				// OBSERVATION: pvc is "Bound", pv is "Bound"
			}
		} else /* pvc.Spec.VolumePtr != nil */ {
			// User asked for a specific PV.
			pv = GetPV(pvc.Spec.VolumePtr)
			if pv == nil {
				// User asked for a PV that does not exist
				// OBSERVATION: pvc is "Pending"
				// Retry later.
				return
			} else if pv.Spec.ClaimPtr == nil {
				// User asked for a PV that is not claimed
				// OBSERVATION: pvc is "Pending", pv is "Available"
				pv.Spec.ClaimPtr = pvc
				pv.Spec.ClaimPtr.UID = pvc.UID
				pv.Annotations[annBoundByController] = "yes"
				pv.Status.Phase = Bound
				if err := CommitPV(pv); err != nil {
					// Retry later.
					return
				}
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				pvc.Annotations[annWasEverBound] = "yes"
				pvc.Status.Phase = Bound
				if err := CommitPVC(pvc); err != nil {
					// Retry later.
					return
				}
				// OBSERVATION: pvc is "Bound", pv is "Bound"
			} else if pv.Spec.ClaimPtr == pvc {
				// User asked for a PV that is claimed by this PVC
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				pv.ClaimPtr.UID = pvc.UID
				pv.Status.Phase = Bound
				if err := CommitPV(pv); err != nil {
					// Retry later.
					return
				}
				pvc.Annotations[annWasEverBound] = "yes"
				pvc.Status.Phase = Bound
				if err := CommitPVC(pvc); err != nil {
					// Retry later.
					return
				}
				// OBSERVATION: pvc is "Bound", pv is "Bound"
			} else {
				// User asked for a PV that is claimed by someone else
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				if pvc.Annotations[annBoundByController] == "" {
					// User asked for a specific PV, retry later
					return
				} else {
					// This should never happen because we set the PVC->PV
					// link with the "established" annotation.
					LogError("IMPOSSIBURU!")
				}
			}
		}
	} else /* pvc.Annotations[annWasEverBound] != "" */ {
		// This PVC has previously been bound
		// OBSERVATION: pvc is not "Pending"
		if pvc.Spec.VolumePtr == nil {
			// Claim was bound before but not any more.
			pvc.Status.Phase = Lost
			if err := CommitPVC(pvc); err != nil {
				// Retry later.
				return
			}
		}
		pv = GetPV(pvc.Spec.VolumePtr)
		if pv == nil {
			// Claim is bound to a non-existing volume.
			pvc.Status.Phase = Lost
			if err := CommitPVC(pvc); err != nil {
				// Retry later.
				return
			}
		} else if pv.Spec.ClaimPtr == nil {
			// Claim is bound but volume has come unbound.
			// This is really a race with other PVCs; it may not work.
			Event("PVClaim is bound to PV, but not vice-versa: attempting to fix it")
			pv.Spec.ClaimPtr = pvc
			pv.Spec.ClaimPtr.UID = pvc.UID
			pv.Status.Phase = Bound
			if err := CommitPV(pv); err != nil {
				// Retry later.
				return
			}
		} else if pv.Spec.ClaimPtr.UID == pvc.UID {
			// All is well
			pv.Status.Phase = Bound
			if err := CommitPV(pv); err != nil {
				// Retry later.
				return
			}
		} else {
			// Claim is bound but volume has a different claimant.
			// Set the claim phase to 'Lost', which is a terminal
			// phase.
			pvc.Status.Phase = Lost
			if err := CommitPVC(pvc); err != nil {
				// If this fails, we will fall back into the enclosing block
				// during the next call to syncPVC; retry later.
				return
			}
		}
	}
}

// FIXME: consider a rogue master
// FIXME: does master election help at all?
// FIXME: extract status setting from spec setting

// This must be async-safe, idempotent, and crash/restart safe, since it
// happens in a loop as well as on-demand.
func syncPV(pv *PV) {
	if pv.Spec.ClaimPtr == nil {
		// Volume is unused
		pv.Status.Phase = Available
		if err := CommitPV(pv); err != nil {
			// Retry later.
			return
		}
		return
	} else /* pv.Spec.ClaimPtr != nil */ {
		// Volume is bound to a claim.
		if pv.Spec.ClaimPtr.UID == 0 {
			// The PV is reserved for a PVC; that PVC has not yet been
			// bound to this PV; the PVC sync will handle it.
			return
		}
		// Get the PVC by _name_
		pvc = GetPVC(pv.Spec.ClaimPtr)
		if pvc.UID != pv.Spec.ClaimPtr.UID {
			// The claim that the PV was pointing to was deleted, and
			// another with the same name created.
			pvc = nil
		}

		if pvc == nil {
			// If we get into this block, the claim must have been deleted;
			// NOTE: releasePV may either release the PV back into the pool or
			// recycle it or do nothing (retain)

			// HOWTO RELEASE A PV
			if pv.Spec.ReclaimPolicy == "Retain" {
				pv.Status.Phase = "released"
				if err := CommitPV(pv); err != nil {
					// retry later
					return
				}
				return
			} else if pv.Spec.ReclaimPolicy == "Delete" {
				plugin := findDeleterPluginForPV(pv)
				if plugin != nil {
					// maintain a map with the current deleter goroutines that are running
					// if the key is already present in the map, return
					//
					// launch the goroutine that:
					// 1. deletes the storage asset
					// 2. deletes the PV API object
					// 3. deletes itself from the map when it's done
				} else {
					// make an event calling out that no deleter was configured
					// mark the PV as failed
				}
			} else if pv.Spec.ReclaimPolicy == "Recycle" {
				plugin := findRecyclerPluginForPV(pv)
				if plugin != nil {
					// maintain a map of running scrubber-pod-monitoring
					// goroutines, guarded by mutex
					//
					// launch a goroutine that:
					// 0. verify the PV object still needs to be recycled or return
					// 1. launches a scrubber pod; the pod's name is deterministically created based on PV uid
					// 2. if the pod is rejected for dup, adopt the existing pod
					// 2.5. if the pod is rejected for any other reason, retry later
					// 3. else (the create succeeds), ok
					// 4. wait for pod completion
					// 5. marks the PV API object as available
					// 6. deletes itself from the map when it's done
				} else {
					// make an event calling out that no recycler was configured
					// mark the PV as failed
				}
			}
		} else if pvc.Spec.VolumePtr == nil {
			// This block collapses into a NOP; we're leaving this here for
			// proof that we don't need this annotation
			if pv.Annotations[annBoundByController] == "yes" {
				// The binding is not completed; let PVC sync handle it
			} else {
				// Dangling PV; possibly^Wprobably re-establish the link in the PVC sync
			}
			return
		} else if pvc.Spec.VolumePtr == pv {
			// Volume is bound to a claim properly.
			// Let the PVC loop handle it.
		} else {
			// Volume is bound to a claim, but the claim is bound elsewhere
			if pv.Annotations[annBoundByController] == "yes" {
				// We did this; fix it.
				pv.Spec.ClaimRef = nil
				pv.Status.Phase = Available
				if err := CommitPV(pv); err != nil {
					// Retry later.
					return
				}
			} else {
				// The PV must have been created with this ptr; leave it alone.
			}
		}
	}
}
