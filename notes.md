# SLIP-Message Notes

- todo

 - queues
  - app-hub
   - configure-subject
    - consumers (names)
    - retention
     - :work-queue (requires use of :next)
     - :all (requires use of :next)
     - :push (normal, ignores consumers)
    - max-messages
    - max-size
   - keep array of queues
    - queue is a struct with subject and envelope of consumer names and message
     - or maybe envelope is interface with retention policy
  - subscriber :next
   - calls to hub

- jetstream-hub-flavor
