// This file represents a high-level view of the primary storage controller (PV
// binding, recycling, and provisioning).
//
// Design:
//
// The fundamental key to this design is the bi-directional "pointer" between
// PersistentVolumes (PVs) and PersistentVolumeClaims (PVCs), which is
// represented here as pvc.Spec.VolumePtr and pv.Spec.ClaimPtr (which are a
// little different in name than the actual Go structs).  The bi-directionality
// is complicated to manage in a transactionless system, but without it we
// can't ensure sane behavior in the face of different forms of trouble.  For
// example, a rogue HA controller instance could end up racing and making
// multiple bindings that are indistinguishable, resulting in potential data
// loss.
//
// This supports pre-bound (by the creator) objects in both directions: a PVC
// that wants a specific PV or a PV that is reserved for a specific PVC.

// This annotation applies to PVCs.  It indicates that the lifecycle of the PVC
// has passed through the initial setup.  This information changes how we
// interpret some observations of the state of the objects.
const annWasEverBound = "pv.kubernetes.io/bound-completed"

// This annotation applies to PVs and PVCs.  It indicates that the binding
// (PV->PVC or PVC->PV) was installed by the controller.  The absence of this
// annotation means the binding was done by the user (i.e. pre-bound).
const annBoundByController = "pv.kubernetes.io/bound-by-controller"

// This annotation represents a new field which instructs dynamic provisioning
// to choose a particular storage class (aka profile).
const annClass = "volume.alpha.kubernetes.io/storage-class"

// This must be async-safe, idempotent, and crash/restart safe, since it
// happens in a loop as well as on-demand.
func SyncPVC(pvc *PVClaim) {
	if !hasAnnotation(pvc, annWasEverBound) {
		// This is a new PVC that has not completed binding
		// OBSERVATION: pvc is "Pending"
		if pvc.Spec.VolumePtr == nil {
			// User did not care which PV they get.
			pv = FindAcceptablePV(pvc) // needs to consider class, etc.
			if pv == nil {
				// No PV could be found
				// OBSERVATION: pvc is "Pending", will retry
				if hasAnnotation(pvc, annClass) {
					plugin := findProvisionerPluginForPV(pv) // Need to flesh this out
					if plugin != nil {
						//FIXME: left off here
						// No match was found and provisioning was requested.
						//
						// maintain a map with the current provisioner goroutines that are running
						// if the key is already present in the map, return
						//
						// launch the goroutine that:
						// 1. calls plugin.Provision to make the storage asset
						// 2. gets back a PV object (partially filled)
						// 3. create the PV API object, with claimRef -> pvc
						// 4. deletes itself from the map when it's done
						// return
					} else {
						// make an event calling out that no provisioner was configured
						// return, try later?
					}
				}
				return
			} else /* pv != nil */ {
				// Found a PV for this claim
				// OBSERVATION: pvc is "Pending", pv is "Available"
				if pv.Spec.ClaimPtr == nil {
					pv.Spec.ClaimPtr = pvc
					pv.Spec.ClaimPtr.UID = pvc.UID
					setAnnotation(pv, annBoundByController)
				}
				pv.Status.Phase = Bound
				if err := CommitPV(pv); err != nil {
					// Nothing was saved; we will fall back into the same
					// condition in the next call to this method
					return
				}
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				pvc.Spec.VolumePtr = pv
				setAnnotation(pvc, annWasEverBound)
				setAnnotation(pvc, annBoundByController)
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
				setAnnotation(pv, annBoundByController)
				pv.Status.Phase = Bound
				if err := CommitPV(pv); err != nil {
					// Retry later.
					return
				}
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				setAnnotation(pvc, annWasEverBound)
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
				setAnnotation(pvc, annWasEverBound)
				pvc.Status.Phase = Bound
				if err := CommitPVC(pvc); err != nil {
					// Retry later.
					return
				}
				// OBSERVATION: pvc is "Bound", pv is "Bound"
			} else {
				// User asked for a PV that is claimed by someone else
				// OBSERVATION: pvc is "Pending", pv is "Bound"
				if !hasAnnotation(pvc, annBoundByController) {
					// User asked for a specific PV, retry later
					return
				} else {
					// This should never happen because we set the PVC->PV
					// link with the "established" annotation.
					LogError("IMPOSSIBURU!")
				}
			}
		}
	} else /* hasAnnotation(pvc, annWasEverBound) */ {
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
// FIXME: extract status setting from spec setting, and convince ourselves we
//        always set status correctly.

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
		if pvc != nil && pvc.UID != pv.Spec.ClaimPtr.UID {
			// The claim that the PV was pointing to was deleted, and
			// another with the same name created.
			pvc = nil
		}

		if pvc == nil {
			// If we get into this block, the claim must have been deleted;
			// NOTE: releasePV may either release the PV back into the pool or
			// recycle it or do nothing (retain)

			// HOWTO RELEASE A PV
			pv.Status.Phase = "released"
			if err := CommitPV(pv); err != nil {
				// retry later
				return
			}
			if pv.Spec.ReclaimPolicy == "Retain" {
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
					// NB: external provisioners/deleters are currently not
					// considered.
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
					// 5.5. clear ClaimRef.UID
					// 5.6. if boundByController, clear ClaimRef & boundByController annotation
					// 6. deletes itself from the map when it's done
				} else {
					// make an event calling out that no recycler was configured
					// mark the PV as failed
				}
			}
		} else if pvc.Spec.VolumePtr == nil {
			// This block collapses into a NOP; we're leaving this here for
			// completeness.
			if hasAnnotation(pv, annBoundByController) {
				// The binding is not completed; let PVC sync handle it
			} else {
				// Dangling PV; try to re-establish the link in the PVC sync
			}
			return
		} else if pvc.Spec.VolumePtr == pv {
			// Volume is bound to a claim properly.
			// Let the PVC loop handle it.
		} else {
			// Volume is bound to a claim, but the claim is bound elsewhere
			if hasAnnotation(pv, annBoundByController) {
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

func initController() {
	// Resync everything because we trust nobody, least of all the people who
	// work on this code.
	Periodically("15s", func() {
		syncAllPVCs()
		syncAllPVs()
	})
	Watch(PVClaims, func(pvc *PVClaim, ev Event) {
		switch ev {
		case MODIFY, CREATE:
			// If a PVC was modified or created, we only need to sync that one.
			syncPVC(pvc)
		case DELETE:
			// If a PVC was deleted, we need to touch the PV it was bound to
			// (if it was bound at all)
			syncPVC(pvc)
			if pvc.Spec.VolumePtr != nil {
				syncPV(pvc.Spec.VolumePtr)
			}
		}
	})
	Watch(PVs, func(pv *PV, ev Event) {
		switch ev {
		case MODIFY:
			// If a PV was modified, we only need to sync that one.
			syncPV(pv)
		case CREATE, DELETE:
			// If a PV was created or deleted we need to re-evaluate all PVCs.
			syncPV(pv)
			syncAllPVCs()
		}
	})
}

func syncAllPVCs() {
	// wait until we have seen an update of both PV and PVC
	// for each pvc {}
}

func syncAllPVs() {
	// wait until we have seen an update of both PV and PVC
	// for each pv {}
}

func hasAnnotation(obj Object, ann string) bool {
	_, found := obj.Annotations[ann]
	return found
}

func setAnnotation(obj Object, ann string) {
	obj.Annotations[ann] = "yes"
}
