------------------------------- MODULE Cleaner --------------------------------
(***************************************************************************)
(* The segment cleaner copying one batch forward while other tasks forget  *)
(* it (crates/mesh/src): `Node::copy_forward` checks the batch is still    *)
(* retained (`Store::retains`), awaits the copy's write (`Wal::copy`), and *)
(* then tells the store (`Store::moved`). The await is where other tasks   *)
(* run: a read trims its stream before it is served (`Node::expire`), and  *)
(* a delete forgets a stream's logs (`Inner::set_floor`). Neither holds    *)
(* the cleaner's lock, which only keeps replacements out.                  *)
(*                                                                         *)
(* One batch: its record in the old segment, and its copy at the head of   *)
(* the log. `contiguous` passes an LSN only once the store knows what is   *)
(* there (a batch it indexed, or a record it will never serve).            *)
(***************************************************************************)
CONSTANT
    Tolerant    \* `moved` takes the copy of a batch forgotten meanwhile as a record never
                \* served (the design). FALSE: it requires the batch to be retained still,
                \* as before reads and deletes could forget one from another task

VARIABLES
    old,        \* the old record: "live", "forgotten" (a trim or a delete), "moved" (its copy took its place)
    copy,       \* the copy: "none", "logged" (in the log; the store not told yet), "live", "dead", "forgotten"
    step,       \* the cleaner: "idle", "checked", "written", "done", "died"
    gone        \* the batch was forgotten: nothing of it may be served again

vars == <<old, copy, step, gone>>

Init == old = "live" /\ copy = "none" /\ step = "idle" /\ gone = FALSE

\* `Store::retains`: the cleaner copies only what is still retained.
Check ==
    /\ step = "idle"
    /\ step' = IF old = "live" THEN "checked" ELSE "done"
    /\ UNCHANGED <<old, copy, gone>>

\* `Wal::copy`, awaited: the copy is in the log.
Write ==
    /\ step = "checked"
    /\ copy' = "logged"
    /\ step' = "written"
    /\ UNCHANGED <<old, gone>>

\* `Store::moved`: the copy takes the batch's place.
Moved ==
    /\ step = "written"
    /\ IF old = "live"
       THEN old' = "moved" /\ copy' = "live" /\ step' = "done"
       ELSE IF Tolerant
            THEN copy' = "dead" /\ step' = "done" /\ UNCHANGED old
            ELSE step' = "died" /\ UNCHANGED <<old, copy>>
    /\ UNCHANGED gone

\* Another task forgets the batch, wherever its live record is: a read's trim, a delete.
Forget ==
    /\ old = "live" \/ copy = "live"
    /\ old' = IF old = "live" THEN "forgotten" ELSE old
    /\ copy' = IF copy = "live" THEN "forgotten" ELSE copy
    /\ gone' = TRUE
    /\ UNCHANGED step

Next == Check \/ Write \/ Moved \/ Forget

Spec == Init /\ [][Next]_vars

\* The retention task goes on: it is what trims, drops segments, checkpoints and evicts.
TaskLives == step # "died"

\* The batch is served from one record at most, and from none once it was forgotten.
ServedOnce == ~(old = "live" /\ copy = "live")
ForgottenIsNotServed == gone => old # "live" /\ copy # "live"

\* Once the cleaner is through, the store knows what is at the copy's LSN: `contiguous` is not
\* held there, and readers go on seeing what is logged after it.
NothingHeldUp == step = "done" => copy # "logged"
===============================================================================
