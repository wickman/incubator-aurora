<!---

* what is the aurora executor

* Architecture
  [ diagram ]

* TaskRunner

  [responsible for launching task, currently the only concrete implementation is thermos ]

* Sandbox

* StatusChecker

  [way of bolting on functionality that should run alongside (possibly) every task]
  examples:
    - disk quota enforcement
    - announcer
    - health checking
    - whatever

* Customizing
    - building your own custom executor with custom entry points
    - writing your own status checker and pants target

-->
