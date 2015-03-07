<!---

* what is thermos
  - thermos is a process workflow manager
  - implemented as a checkpointed state machine
  - thermos itself is stateless -- only acts upon data committed to durable storage (disk)
  - in other words you can kill -9 thermos at any point in time and resume a
    thermos task from where it left off.

* life of a thermos task
  - a collection of processes, temporal constraints between processes, and
    a termination rule to determine success or failure (e.g. task failures.)
  - can be configured either using pystachio or json

  task
  - active states: ACTIVE, CLEANING, FINALIZING
  - terminal states: SUCCESS, FAILED, KILLED, LOST

    () - starting state, [] - terminal state

       .--------------------------------------------+----.
       |                                            |    |
       |                   .----------> [SUCCESS]   |    |
       |                   |                        |    |
       |                   | .--------> [FAILED]    |    |
       |                   | |                      |    |
    (ACTIVE)           FINALIZING ---> [KILLED] <---'    |
       |                 ^    |    .------^              |
       |                 |    |    |                     |
       `---> CLEANING ---'    `----)--> [LOST] <---------'
                | |                |      ^
                | `----------------'      |
                `-------------------------'

    ACTIVE -> KILLED/LOST only happens under garbage collection situations.
    Ordinary task preemption/kill still goes through CLEANING/FINALIZING before
    reaching a terminal state.

  - life of a thermos task:
    ACTIVE     - running the ordinary task plan
    CLEANING   - regular plan has failed finished, currently sending
                 SIGTERMs to whatever remains.  if everything has finished
                 before the finalization wait, then we move onto the finalization
                 state, otherwise we transition directly to terminal
    FINALIZING - run the finalization plan.
    [SUCCESS, FAILED, KILLED, LOST]

  - attributes that control task failure state:
    task: max_failures
    process: ephemeral


  process
      () - starting state, [] - terminal state

      .--------------------> [FAILED]
      |                         ^
      |                         |
  (WAITING) ----> FORKED ----> RUNNING -----> [KILLED]
                    |          |    | 
                    v          |    `---> [SUCCESS]
                 [LOST] <------'

  - attributes that control process failure state:
    max_failures
    daemon
    ephemeral
  
  - life of a thermos process:
    WAITING  - blocked on execution dependencies
    FORKED   - the runner has forked a replica itself -- a coordinator -- and
               is preparing to fork the user-defined process once it has taken
               control of its checkpoint log
    RUNNING  - the user process has been launched and is currently running
    SUCCESS  - the user process succeeded with exit status 0
    KILLED   - the user process was killed either by user action (return code < 0),
               by explicit instruction (thermos kill), or killed because the
               runner was tearing down (examples: ephemeral processes,
               processes that do not respond to SIGTERM, finalizing processes
               that took too long to run.)
    FAILED   - the exit status was >= 0
    LOST     - the coordinator was forked but the process it controls did not transition
               to RUNNING within a certain period of time (usually 60s.)
               if a process goes LOST, a replacement will be forked and it will not count
               against the failure count for this process.
               [NOTE: thar be dragons, if disk fsync is taking >60s, this could result in an
                out of order sequence number condition that will cause the entire task to
                fail.]
  

* thermos cli
  - thermos status
  - thermos run
  - thermos read
  - thermos inspect
  - thermos simplerun
  - thermos tail
  - thermos kill
  - thermos gc

* thermos observer
  - UI, typically run as root (drawbacks of course)
  - detects tasks in a fixed thermos root (variant called the aurora observer that
    is a thermos observer that can detect from multiple roots including mesos sandboxes.)

* thermos root
  - all state for thermos, typically /var/run/thermos
  - $THERMOS_ROOT
    - checkpoints
       - $TASK_ID
          - runner
          - coordinator.$PROCESS_ID
    - tasks
       - active
          - $TASK_ID
       - finished
          - $TASK_ID

* how thermos works

  task description dumped into active/task_id or finished/task_id.  the atomicity of
  os.rename is critical here to determine the state of the task.

  two sets of checkpoint streams:
    runner: the assembled thermos runner checkpoint stream -- the state of the world as
    understood by the thermos runner

    coordinator.*: the coordinator checkpoint stream: the state of the world for a single
    process as understood by that process' coordinator

  the format:
    recordio: 4 bytes big endian long: record size, record.
    the records are RunnerCkpt records encoded using thrift
    reconstructed by apache.thermos.common.ckpt ('CheckpointDispatcher')
    can be inspected using 'thermos read --simple'

  Runner:
    | r0 | TaskStatus ACTIVE, uid, pid, timestamp
    | r1 | RunnerHeader
    | r2 (timeout seq0) |
    | r3 (hello_world seq0) |
    | r4 (timeout seq1) |
    | r5 | TaskStatus ACTIVE, uid, pid', timestamp  <- re-parenting
    | r6 (hello_world seq1) |
      ...
    | rN | TaskStatus CLEANING, uid, pid, timestamp
    | rN+1 | TaskStatus FINALIZING, uid, pid, timestamp
    | rN+2 | TaskStatus KILLED, uid, pid, timestamp
   
  Coordinator:
    hello_world    | seq0 | <handoff> | seq1 | seq2 | ..
    goodbye_world  | seq0 | <handoff> | seq1 | seq2 | ...

  The runner periodically checks its high watermark and the high watermark of
  each coordinator.  If it detects that the coordinator sequence number is higher
  than its own, it takes that checkpoint record and appends it to its own runner
  checkpoint stream.  This generates a state transition event in the runner state machine.
  The runner registers event handlers for some of these state transitions, such as 
  "kill process" or "refork new process" or "terminate entire task."

* how aurora uses thermos

  There is an Aurora executor (aurora-executor.md) that has a generic task runner plugin.
  The only concrete implementation of this plugin is the ThermosTaskRunner.  The ThermosTaskRunner
  simply takes the thermos task (defined in json and serialized in the mesos TaskInfo) and
  forks a variant of the thermos CLI that runs a task, then monitors the Runner checkpoint (as described above)
  to determine what the current state of the task is.

-->